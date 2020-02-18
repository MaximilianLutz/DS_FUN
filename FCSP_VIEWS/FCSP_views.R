library(dplyr)
library(jsonlite)
library("prophet")
file <- fromJSON('/Users/maximilian.lutz/Downloads/response_1582039196521.json')
file <- as.data.frame(file)
df <- select(file, items.timestamp, items.views)
df <- rename(df, ds = items.timestamp, y = items.views)
df$ds = substr(df$ds,1,nchar(df$ds)-2)
df$ds <- as.Date(df$ds, format = '%Y%m%d')


m <- prophet(df)
future <- make_future_dataframe(m, periods = 365)
tail(future)

forecast <- predict(m, future)
tail(forecast[c('ds', 'yhat', 'yhat_lower', 'yhat_upper')])
plot(m, forecast)

prophet_plot_components(m, forecast)
