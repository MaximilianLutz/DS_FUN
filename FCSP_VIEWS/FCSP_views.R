library(dplyr)
library(jsonlite)
library(prophet)
# Import Data
file <- fromJSON('/Users/maximilian.lutz/Downloads/response_1582039196521.json')
file <- as.data.frame(file)

# Prophet need a DF to be structured like this:
df <- select(file, items.timestamp, items.views)
df <- rename(df, ds = items.timestamp, y = items.views)

# Correcting the Date format:
df$ds = substr(df$ds,1,nchar(df$ds)-2)
df$ds <- as.Date(df$ds, format = '%Y%m%d')

# Create the model
m <- prophet(df)

# Predictions are easy:
future <- make_future_dataframe(m, periods = 365)
tail(future)
forecast <- predict(m, future)
tail(forecast[c('ds', 'yhat', 'yhat_lower', 'yhat_upper')])

# Plotting is easy, too
plot(m, forecast)

# This is insanely useful: 
prophet_plot_components(m, forecast)
