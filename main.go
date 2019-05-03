package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

	if *allTables {
		showAllTables(*regionFlag, day)
		return
	}

	if *tableFlag == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	showSingleTable(*regionFlag, *tableFlag, day)
}

func showAllTables(region string, day time.Time) {
	tables, err := getTableNames(region)
	if err != nil {
		fmt.Printf("unable to get table names for region %s: %v\n", region, err)
		os.Exit(1)
	}
	var summaries []tableCosts
	for _, t := range tables {
		s, err := getTableCosts(region, t, day)
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

func showSingleTable(region, table string, day time.Time) {
	s, err := getTableCosts(region, table, day)
	if err != nil {
		fmt.Printf("unable to get summary for %s: %v\n", table, err)
		os.Exit(1)
	}
	fmt.Print(s.String())
}

func newtableCosts(table string, readMetrics, writeMetrics []dynamoDBMetric) (s tableCosts) {
	s.table = table
	var reads, writes float64
	for _, m := range readMetrics {
		reads += m.Consumed
	}
	for _, m := range writeMetrics {
		writes += m.Consumed
	}


	var pricing, err = getPricing()

	if err != nil {
		return
	}

	// Read request units (price per million)
	s.onDemandRead += pricing.onDemandRead * (reads / float64(1000000))
	// Write request units (price per million)
	s.onDemandWrite = pricing.onDemandWrite * (writes / float64(1000000))

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
	onDemandRead, onDemandWrite, readCapacityUnit, writeCapacityUnit float64
}

type dynamoPricingResponse struct {
	PriceList []string `json:"PriceList"`
}

type priceDetail struct {
	Product     product `json:"product"`
	ServiceCode string  `json:"serviceCode"`
	Terms       terms   `json:"terms"`
}

type product struct {
	ProductFamily string `json:"productFamily"`
}

type terms struct {
}

func getPricing() (s dynamoPricing, err error) {
	// Pricing data only available in 2 regions
	region := "us-east-1"

	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}

	svc := pricing.New(sess)

	pricingReq := &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonDynamoDB"),
	}

	pricing, err := svc.GetProducts(pricingReq)

	if err != nil {
		return
	}

	fmt.Print(pricing.PriceList)

	s.onDemandRead = 0.297
	s.onDemandWrite = 1.4846

	s.readCapacityUnit = 0.0001544
	s.writeCapacityUnit = 0.000772

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

func getTableCosts(region, table string, day time.Time) (s tableCosts, err error) {
	rm, err := getReadMetrics(region, table, day)
	if err != nil {
		return
	}
	wm, err := getWriteMetrics(region, table, day)
	if err != nil {
		return
	}
	s = newtableCosts(table, rm, wm)
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
