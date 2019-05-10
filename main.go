package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/pricing"
)

var tableFlag = flag.String("table", "", "Name of the table to query")
var allTables = flag.Bool("allTables", false, "Query all tables and produce a summary of potential cost savings")
var regionFlag = flag.String("region", "eu-west-2", "AWS region name")
var dayFlag = flag.String("day", "2019-04-01", "Day to base values on")

func main() {
	flag.Parse()

	day, err := time.Parse("2006-01-02", *dayFlag)
	if err != nil {
		fmt.Printf("unexpected date format: %v\n", err)
		os.Exit(1)
	}

	location := endpoints.AwsPartition().Regions()[*regionFlag].Description()
	pricing, err := getDynamoPricing(location)
	if err != nil {
		fmt.Printf("failed to fetch current pricing\n")
		os.Exit(1)
	}

	if *allTables {
		showAllTables(*regionFlag, day, pricing)
		return
	}

	if *tableFlag == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	showSingleTable(*regionFlag, *tableFlag, day, pricing)
}

func showAllTables(region string, day time.Time, pricing dynamoPricing) {
	tables, err := getTableNames(region)
	if err != nil {
		fmt.Printf("unable to get table names for region %s: %v\n", region, err)
		os.Exit(1)
	}
	var summaries []tableCosts
	for _, t := range tables {
		s, err := getTableCosts(region, t, day, pricing)
		if err != nil {
			if err != nil {
				fmt.Printf("unable to get summary for %s: %v\n", t, err)
				os.Exit(1)
			}
		}
		summaries = append(summaries, s)
		fmt.Println(s.String())
	}

	var costSaving float64
	for _, s := range summaries {
		saving := s.provisionedCost() - s.onDemandCost()
		if saving > 0 {
			fmt.Printf("Switch table %s to on-demand to save $%.5f per day\n", s.table, saving)
			costSaving += saving
		}
	}
	fmt.Println()
	fmt.Printf("Total saved per day: $%.5f\n", costSaving)
	fmt.Printf("Total saved per month: $%.5f\n", (costSaving*365)/12)
}

func showSingleTable(region, table string, day time.Time, pricing dynamoPricing) {
	s, err := getTableCosts(region, table, day, pricing)
	if err != nil {
		fmt.Printf("unable to get summary for %s: %v\n", table, err)
		os.Exit(1)
	}
	fmt.Print(s.String())
}

func newtableCosts(table string, readMetrics, writeMetrics []dynamoDBMetric, pricing dynamoPricing) (s tableCosts) {
	s.table = table
	var reads, writes float64
	for _, m := range readMetrics {
		reads += m.Consumed
	}
	for _, m := range writeMetrics {
		writes += m.Consumed
	}

	// Read request units
	s.onDemandRead += pricing.onDemandRead * reads
	// Write request units
	s.onDemandWrite = pricing.onDemandWrite * writes

	// Read capacity unit (RCU)
	for _, m := range readMetrics {
		s.provisionedRead += m.ProvisionedUnits * pricing.readCapacityUnit
	}
	// Write capacity unit (WCU)
	for _, m := range writeMetrics {
		s.provisionedWrite += m.ProvisionedUnits * pricing.writeCapacityUnit
	}
	return
}

type dynamoPricing struct {
	location                            string
	onDemandRead, onDemandWrite         float64
	readCapacityUnit, writeCapacityUnit float64
}

type priceDetail struct {
	Product     product
	ServiceCode string
	Terms       terms
}

type product struct {
	ProductFamily string
	Sku           string
	Attributes    attributes
}

type attributes struct {
	Location string
}

type terms struct {
	OnDemand map[string]interface{}
}

type onDemandTerms struct {
	PriceDimensions map[string]interface{}
}

type onDemandDetails struct {
	Description  string
	PricePerUnit pricePerUnit
}

type pricePerUnit struct {
	Usd string
}

func parseProduct(product aws.JSONValue) (pice float64, err error) {
	var result priceDetail
	err = mapstructure.Decode(product, &result)
	if err != nil {
		return
	}

	if len(result.Terms.OnDemand) > 1 {
		return 0, errors.New("more than one product term found")
	}

	for _, v := range result.Terms.OnDemand {

		var result2 onDemandTerms
		err = mapstructure.Decode(v, &result2)
		if err != nil {
			return
		}

		for _, v2 := range result2.PriceDimensions {

			var result3 onDemandDetails

			err = mapstructure.Decode(v2, &result3)
			if err != nil {
				return
			}

			price, err := strconv.ParseFloat(result3.PricePerUnit.Usd, 64)

			if err != nil {
				err = fmt.Errorf("failed to parse on-demand pricing: %v", err)
				fmt.Println(err)
				return 0, err
			}

			// Return first non-zero to ignore any free-tier pricing
			if price > 0.00 {
				return price, nil
			}

		}
	}

	return 0, errors.New("no product data found")
}

func getDynamoPricing(location string) (s dynamoPricing, err error) {
	provisionedReadGroupDescription := "DynamoDB Provisioned Read Units"
	provisionedWriteGroupDescription := "DynamoDB Provisioned Write Units"
	onDemandReadGroupDescription := "DynamoDB PayPerRequest Read Request Units"
	onDemandWriteGroupDescription := "DynamoDB PayPerRequest Write Request Units"

	readCapacityUnit, err := getPrice(location, provisionedReadGroupDescription)
	if err != nil {
		return
	}

	writeCapacityUnit, err := getPrice(location, provisionedWriteGroupDescription)
	if err != nil {
		return
	}

	onDemandRead, err := getPrice(location, onDemandReadGroupDescription)
	if err != nil {
		return
	}

	onDemandWrite, err := getPrice(location, onDemandWriteGroupDescription)
	if err != nil {
		return
	}

	s.location = location
	s.readCapacityUnit = readCapacityUnit
	s.writeCapacityUnit = writeCapacityUnit
	s.onDemandRead = onDemandRead
	s.onDemandWrite = onDemandWrite

	return
}

func (dp dynamoPricing) String() string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Current prices for %s:\n", dp.location))
	b.WriteString(fmt.Sprintf("  On Demand:\n"))
	b.WriteString(fmt.Sprintf("    Reads:   $%.5f (per million)\n", dp.onDemandRead*float64(1000000)))
	b.WriteString(fmt.Sprintf("    Writes:  $%.5f (per million)\n", dp.onDemandWrite*float64(1000000)))
	b.WriteString(fmt.Sprintf("  Provisioned:\n"))
	b.WriteString(fmt.Sprintf("    RCUs:    $%.5f\n", dp.readCapacityUnit))
	b.WriteString(fmt.Sprintf("    WCUs:    $%.5f\n", dp.writeCapacityUnit))
	return b.String()
}

func getPrice(location, groupDescription string) (res float64, err error) {
	// Pricing data endpoint only available in 2 regions
	// the other is ap-south-1
	region := endpoints.UsEast1RegionID

	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}

	svc := pricing.New(sess)

	var priceFilters []*pricing.Filter
	priceFilters = append(priceFilters, &pricing.Filter{
		Type:  aws.String(pricing.FilterTypeTermMatch),
		Field: aws.String("location"),
		Value: aws.String(location),
	})

	priceFilters = append(priceFilters, &pricing.Filter{
		Type:  aws.String(pricing.FilterTypeTermMatch),
		Field: aws.String("groupDescription"),
		Value: aws.String(groupDescription),
	})

	pricingReq := &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonDynamoDB"),
		Filters:     priceFilters,
	}

	pricing, err := svc.GetProducts(pricingReq)

	if err != nil {
		return
	}

	for _, product := range pricing.PriceList {
		res, err := parseProduct(product)

		if err != nil {
			err = fmt.Errorf("failed to parse product")
			return 0, err
		}

		return res, nil

	}

	return
}

type tableCosts struct {
	table                             string
	onDemandRead, onDemandWrite       float64
	provisionedRead, provisionedWrite float64
}

func (tc tableCosts) onDemandCost() float64 {
	return tc.onDemandRead + tc.onDemandWrite
}

func (tc tableCosts) provisionedCost() float64 {
	return tc.provisionedRead + tc.provisionedWrite
}

func (tc tableCosts) String() string {
	var b bytes.Buffer
	b.WriteString(tc.table)
	b.WriteString("\n")
	b.WriteString("  Estimated daily:\n")
	b.WriteString(fmt.Sprintf("    On Demand Read: $%.5f\n", tc.onDemandRead))
	b.WriteString(fmt.Sprintf("    On Demand Write: $%.5f\n", tc.onDemandWrite))
	b.WriteString(fmt.Sprintf("    Provisioned Read: $%.5f\n", tc.provisionedRead))
	b.WriteString(fmt.Sprintf("    Provisioned Write: $%.5f\n", tc.provisionedWrite))
	b.WriteString("  Estimated monthly:\n")
	b.WriteString(fmt.Sprintf("    On Demand Read: $%.5f\n", (tc.onDemandRead*365)/12))
	b.WriteString(fmt.Sprintf("    On Demand Write: $%.5f\n", (tc.onDemandWrite*365)/12))
	b.WriteString(fmt.Sprintf("    Provisioned Read: $%.5f\n", (tc.provisionedRead*365)/12))
	b.WriteString(fmt.Sprintf("    Provisioned Write: $%.5f\n", (tc.provisionedWrite*365)/12))
	return b.String()
}

func getTableCosts(region, table string, day time.Time, pricing dynamoPricing) (s tableCosts, err error) {
	rm, err := getReadMetrics(region, table, day)
	if err != nil {
		return
	}
	wm, err := getWriteMetrics(region, table, day)
	if err != nil {
		return
	}
	s = newtableCosts(table, rm, wm, pricing)

	return
}

type dynamoDBMetric struct {
	Provisioned      float64
	ProvisionedUnits float64
	Consumed         float64
	ConsumedUnits    float64
}

type metric string

const metricProvisionedReadCapacity metric = "ProvisionedReadCapacityUnits"
const metricConsumedReadCapacity metric = "ConsumedReadCapacityUnits"
const metricProvisionedWriteCapacity metric = "ProvisionedWriteCapacityUnits"
const metricConsumedWriteCapacity metric = "ConsumedWriteCapacityUnits"

type stat string

const statAverage stat = "Average"
const statMax stat = "Maximum"
const statSum stat = "Sum"
const statSampleCount stat = "SampleCount"

func getReadMetrics(region, tableName string, day time.Time) (metrics []dynamoDBMetric, err error) {
	return getProvisionedAndConsumedMetrics(region, tableName, day,
		metricProvisionedReadCapacity, metricConsumedReadCapacity)
}

func getWriteMetrics(region, tableName string, day time.Time) (metrics []dynamoDBMetric, err error) {
	return getProvisionedAndConsumedMetrics(region, tableName, day,
		metricProvisionedWriteCapacity, metricConsumedWriteCapacity)
}

func getProvisionedAndConsumedMetrics(region, tableName string, day time.Time, provisionedMetric, consumedMetric metric) (metrics []dynamoDBMetric, err error) {
	provisioned, err := getMetrics(region, tableName, provisionedMetric, day)
	if err != nil {
		return
	}
	consumed, err := getMetrics(region, tableName, consumedMetric, day)
	if err != nil {
		return
	}
	maxlen := len(provisioned.Datapoints)
	if len(consumed.Datapoints) > maxlen {
		maxlen = len(consumed.Datapoints)
	}
	metrics = make([]dynamoDBMetric, maxlen)
	for i, pd := range provisioned.Datapoints {
		metrics[i].Provisioned = *pd.Sum
		metrics[i].ProvisionedUnits = *pd.Average
	}
	for i, cd := range consumed.Datapoints {
		metrics[i].Consumed = *cd.Sum
		metrics[i].ConsumedUnits = *cd.Average
	}
	return
}

func getTableNames(region string) (tables []string, err error) {
	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}
	svc := dynamodb.New(sess)
	err = svc.ListTablesPages(&dynamodb.ListTablesInput{}, func(lto *dynamodb.ListTablesOutput, lastPage bool) bool {
		for _, s := range lto.TableNames {
			tables = append(tables, *s)
		}
		return true // continue
	})
	return
}

func getMetrics(region, tableName string, metric metric, day time.Time) (metrics *cloudwatch.GetMetricStatisticsOutput, err error) {
	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}
	svc := cloudwatch.New(sess)
	metrics, err = svc.GetMetricStatistics(&cloudwatch.GetMetricStatisticsInput{
		MetricName: aws.String(string(metric)),
		StartTime:  aws.Time(day),
		EndTime:    aws.Time(day.Add(time.Hour * 24)),
		Period:     aws.Int64(3600), // 1 hour
		Statistics: []*string{aws.String(string(statAverage)),
			aws.String(string(statMax)),
			aws.String(string(statSum)),
			aws.String(string(statSampleCount)),
		},
		Namespace: aws.String("AWS/DynamoDB"),
		Dimensions: []*cloudwatch.Dimension{
			&cloudwatch.Dimension{
				Name:  aws.String("TableName"),
				Value: aws.String(tableName),
			},
		},
	})
	return
}
