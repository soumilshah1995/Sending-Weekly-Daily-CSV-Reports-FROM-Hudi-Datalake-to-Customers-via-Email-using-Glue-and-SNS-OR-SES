
Sending Weekly /Daily CSV Reports  FROM  Hudi Datalake to Customers via Email using Glue and SNS OR SES


![Capture](https://github.com/soumilshah1995/Sending-Weekly-Daily-CSV-Reports-FROM-Hudi-Datalake-to-Customers-via-Email-using-Glue-and-SNS-OR-SES/assets/39345855/ed46f51f-1d16-4991-8d11-8997d483b1aa)

# Template 
```
 loaders = {
        "source": [
            {
                "table_name": "orders",
                "hudi_path": "s3://XXX/silver/table_name=orders",
                "type": "FULL"  # FULL | INC
            },
            {
                "table_name": "customers",
                "hudi_path": "s3://XXXX/silver/table_name=customers",
                "type": "FULL"  # FULL | INC
            }
        ],
        "transform": {
            "query": """SELECT o.*,
            c.name AS customer_name,
        c.email AS customer_email
            FROM orders AS o
            JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.priority = 'URGENT' """
        },
        "email": {
            "sender_email": "sXXXX",
            "recipient_email": "XXXXom",
            "subject": "Download Link for Data",
            "report_bucket": "hudXXX5",
            "file_expires_in": 86400
        }

    }
```

# Output 
![image](https://github.com/soumilshah1995/Sending-Weekly-Daily-CSV-Reports-FROM-Hudi-Datalake-to-Customers-via-Email-using-Glue-and-SNS-OR-SES/assets/39345855/fbd6a15c-e99b-4c5b-916e-c2da3359f022)
