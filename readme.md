# Pipeline to follow twitter users timelines

### 1) Retrieves a list of twitter accounts to follow from a Google sheet.
### 2) Retrieves the latest tweets of these accounts (up to 3200 - constraint of the Twitter API)
### 3) Inserts into a MongoDb database
### 4) Inserts the checked dates in the Google sheet

Gets the newest tweets when the checked date is not empty or inferior to today's date.

![Pipeline](./pipeline.png)