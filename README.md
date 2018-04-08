# Gun Violence Archive Data

## What is this repository?

This repository contains data for all recorded gun violence incidents in the US between January 2013 and March 2018, inclusive.

## Where did you get the data?

The data was downloaded from [gunviolencearchive.org](http://www.gunviolencearchive.org/). From the organization's description:

> Gun Violence Archive (GVA) is a not for profit corporation formed in 2013 to provide free online public access to accurate information about gun-related violence in the United States. GVA will collect and check for accuracy, comprehensive information about gun-related violence in the U.S. and then post and disseminate it online.

## How did you get the data?

Because GVA limits the number of incidents that are returned from a single query, and because the website's "Export to CSV" functionality was missing crucial fields, it was necessary to obtain this dataset using web scraping techniques.

**Stage 1:** For each date between 1/1/2013 and 3/31/2018, a Python script queried all incidents that happened at that particular date, then scraped the data and wrote it to a CSV file. Each month got its own CSV file, with the exception of 2013, since not many incidents were recorded from then.

**Stage 2:** Each entry was augmented with additional data not directly viewable from the query results page, such as participant information, geolocation data, etc.

**Stage 3:** The entries were sorted in order of increasing date, then merged into a single CSV file.
