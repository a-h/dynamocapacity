# dynamocapacity

Compare cost of DynamoDB On Demand versus your current Provisioned Capacity setup.

## Get stats for a single table.

```
go run main.go -region=eu-west-2 -table=your-table-name -day=2019-05-02
```

```
your-table-name
  Estimated daily:
    On Demand Read: $0.00075
    On Demand Write: $0.00075
    Provisioned Read: $0.01930
    Provisioned Write: $0.09264
  Estimated monthly:
    On Demand Read: $0.02282
    On Demand Write: $0.02282
    Provisioned Read: $0.58704
    Provisioned Write: $2.81780
```

## Get stats for a region.

```
go run main.go -region=eu-west-2 -allTables=true -day=2019-05-02
```

```
your-table-name
  Estimated daily:
    On Demand Read: $0.00075
    On Demand Write: $0.00075
    Provisioned Read: $0.01930
    Provisioned Write: $0.09264
  Estimated monthly:
    On Demand Read: $0.02282
    On Demand Write: $0.02282
    Provisioned Read: $0.58704
    Provisioned Write: $2.81780
    
your-table-name2
  Estimated daily:
    On Demand Read: $0.00075
    On Demand Write: $0.00075
    Provisioned Read: $0.01930
    Provisioned Write: $0.09264
  Estimated monthly:
    On Demand Read: $0.02282
    On Demand Write: $0.02282
    Provisioned Read: $0.58704
    Provisioned Write: $2.81780
    
Switch table your-table-name to on-demand to save $0.11044 per day
Switch table your-table-name2 to on-demand to save $0.11044 per day

Total saved per day: $0.22088
Total saved per month: $9.674544
```
