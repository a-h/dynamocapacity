package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

var tableFlag = flag.String("table", "", "Name of the table to query")
var regionFlag = flag.String("region", "eu-west-2", "AWS region name")
var dayFlag = flag.String("day", "2019-04-01", "Day to base values on")

func main() {
	flag.Parse()

	day, err := time.Parse("2006-01-02", *dayFlag)
	if err != nil {
		fmt.Printf("unexpected date format: %v\n", err)
		os.Exit(1)
	}

	if *tableFlag == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	rm, err := getReadMetrics(*regionFlag, *tableFlag, day)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wm, err := getWriteMetrics(*regionFlag, *tableFlag, day)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Reads: %+v\n", rm)
	fmt.Printf("Writes: %+v\n", wm)
	fmt.Println()
	onDemandRead, onDemandWrite, provisionedRead, provisionedWrite := costs(rm, wm)
	fmt.Println("Estimated daily:")
	fmt.Printf("On Demand Read: $%.5f\n", onDemandRead)
	fmt.Printf("On Demand Write: $%.5f\n", onDemandWrite)
	fmt.Printf("Provisioned Read: $%.5f\n", provisionedRead)
	fmt.Printf("Provisioned Write: $%.5f\n", provisionedWrite)
	fmt.Println("Estimated monthly:")
	fmt.Printf("On Demand Read: $%.5f\n", (onDemandRead*365)/12)
	fmt.Printf("On Demand Write: $%.5f\n", (onDemandWrite*365)/12)
	fmt.Printf("Provisioned Read: $%.5f\n", (provisionedRead*365)/12)
	fmt.Printf("Provisioned Write: $%.5f\n", (provisionedWrite*365)/12)
}

func costs(readMetrics, writeMetrics []dynamoDBMetric) (onDemandRead, onDemandWrite float64,
	provisionedRead, provisionedWrite float64) {
	var reads, writes float64
	for _, m := range readMetrics {
		reads += m.Consumed
	}
	for _, m := range writeMetrics {
		writes += m.Consumed
	}
	// Read request units	$0.297 per million read request units
	onDemandRead += 0.297 * (reads / float64(1000000))
	// Write request units	$1.4846 per million write request units
	onDemandWrite = 0.297 * (reads / float64(1000000))

	// Read capacity unit (RCU)	$0.0001544 per RCU
	for _, m := range readMetrics {
		provisionedRead += m.ProvisionedUnits * 0.0001544
	}
	// Write capacity unit (WCU)	$0.000772 per WCU
	for _, m := range writeMetrics {
		provisionedWrite += m.ProvisionedUnits * 0.000772
	}
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
	if len(provisioned.Datapoints) != len(consumed.Datapoints) {
		err = fmt.Errorf("count of provisioned and consumed datapoints didn't match (%d and %d)",
			len(provisioned.Datapoints),
			len(consumed.Datapoints))
		return
	}
	metrics = make([]dynamoDBMetric, len(provisioned.Datapoints))
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
