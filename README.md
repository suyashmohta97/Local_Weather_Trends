# Local Weather Trends

<img width="1465" height="819" alt="image" src="https://github.com/user-attachments/assets/621f8917-55d6-4d04-a487-acafb9ab32dc" />


## Local Weather Trends Dashboard (BigQuery plus Power BI)

### Project background
The objective was to give local communities an always-up-to-date view of upcoming conditions. By streaming open-access forecast data into BigQuery every three hours and pushing a clean model to Power BI, the dashboard lets users explore temperature, rain, humidity and wind for any city in the source feed.

### Problem statement
Weather sites give rich global maps but very little slice-and-dice capability. Small businesses and event planners need city specific, hour-level trends they can filter by date without copying CSV files every day. The project set out to build a fully automated pipeline that refreshes raw data, transforms it for analytics and visualises it in real time.

### Key questions

• How will temperature, rain, humidity and wind change during the next seven days in a chosen city

• Which hours show the highest rain probability so that outdoor work can be rescheduled

• How does current week humidity compare with the previous four weeks for the same location

• What wind patterns are emerging that might affect logistics or air quality alerts

### Data overview

• Source API: Open-weather endpoint queried in JSON format

• Frequency: Three hour forecasts for the next five days, refreshed every three hours via Cloud Scheduler

• Landing table: BigQuery weather_data.weather_snapshot with raw nested JSON flattened to atomic columns

• Fields: datetime_utc, city_id, city_name, temperature_k, humidity_pct, wind_speed_mps, rain_mm, clouds_pct

## Methodology

### Ingestion

 • Cloud Function triggers the API call, writes the result to BigQuery as incremental append

### Transformation

 • BigQuery SQL view converts Kelvin to Celsius and Fahrenheit, converts metres per second to kilometres per hour, and flags daytime vs nighttime records using TIMESTAMP functions

### Visualisations

<img width="681" height="442" alt="image" src="https://github.com/user-attachments/assets/e751fc39-1750-4595-8cf7-759ee09d6f50" />



Temperature Trends - This chart compares actual temperature and feels-like temperature across 3-hour intervals. Users can observe heat spikes, nighttime drops, and weather comfort patterns over the forecast horizon.




<img width="682" height="438" alt="image" src="https://github.com/user-attachments/assets/15c41c97-894e-45f0-b6ec-8cfe08f6e5cf" />


Wind Speed Analysis - Wind speed variation is visualized over time to detect gust patterns that might impact logistics or safety alerts.

 • Power BI desktop connects to BigQuery using direct query so visuals update whenever the table does
 
 • Report pages: City Overview, Hourly Breakdown, Seven-Day Trend, Comparative Analysis
 
 • Filters: city name, date range, metric selector
 

### Automation

 • Cloud Scheduler orchestrates the refresh so no manual steps are needed once deployed

### Key findings and insights (example from a recent seven day slice)

• Afternoon temperature peaks are two to three °C higher than the same hours last week in San Diego

• Rain probability exceeds forty percent in Seattle on 2025-07-28 between 09:00 and 15:00 UTC, suggesting morning outdoor events should shift indoors

• Humidity in Houston remains above sixty five percent for fifty percent of the forecast horizon, a signal for retailers to adjust in-store cooling loads

• Wind gusts above thirty kilometres per hour cluster overnight in Chicago, likely impacting early cargo flights

### Conclusion


The pipeline delivers granular, city level insight without manual exports. The direct query connection means Power BI tiles refresh moments after each ingestion, turning BigQuery into the single source of truth for operational weather intelligence.

### Future prospects 


• Add severe weather alerts by joining the snapshot to National Weather Service warning feeds

• Build DAX measures that convert UTC to the viewer’s local time for better readability

• Enable row level security in Power BI so each business unit sees only its relevant cities

• Archive daily snapshots to a separate table for long term trend mining
