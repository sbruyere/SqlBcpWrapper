# Overview

[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/sbruyere/SqlBcpWrapper/master/LICENSE)
![](https://img.shields.io/badge/language-C%23-blue.svg)

This library is a SQL BCP wrapper library for .Net application. It has been designed to copy the result set from a Transact-SQL statement to a file or a stream directly from your .Net code.

When performing database maintenance you will certainly need to export data out of your database tables to a storage or directly in memory. 

Microsoft SQL Server includes a popular command-line utility named BCP for quickly bulk copying large files into tables or views in SQL Server databases.


## Usage

```c#
        private static Stream SqlBcpDownload(DateTime startDate, DateTime endDate, int elementID)
        {
            const string SQL_RQST = "SELECT * FROM [{0}].[dbo].[UserSubscription] WHERE SubscriptionDate >= '{1:yyyy-MM-dd}' AND SubscriptionDate < '{2:yyyy-MM-dd}' AND ElementID = {3:d}";

            var sqlBcpWrapper = new SqlBcpWrapper("SqlServerName", "SqlUserName", "SqlUserPwd");

            sqlBcpWrapper.OutputReceived += SqlBcpWrapper_OutputReceived;
            sqlBcpWrapper.ErrorReceived += SqlBcpWrapper_ErrorReceived;

            Stream myStream = new MemoryStream();

            sqlBcpWrapper.Run(myStream,
                SQL_RQST,
                "TargetDatabaseName",
                startDate.ToString(CultureInfo.InvariantCulture),
                endDate.ToString(CultureInfo.InvariantCulture),
                elementID.ToString());

            return myStream;

        }
        
        private static void SqlBcpWrapper_ErrorReceived(object sender, DataReceivedEventArgs e)
        {
            Console.WriteLine(@"Error: {0}", e.Data);
            throw new Exception(e.Data);
        }

        private static void SqlBcpWrapper_OutputReceived(object sender, DataReceivedEventArgs e)
        {
            Console.WriteLine(@"# {0}", e.Data);
        }
```
