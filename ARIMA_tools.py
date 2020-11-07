from statsmodels.tsa.statespace.sarimax import SARIMAX
import statsmodels.api as sm
import matplotlib.pyplot as plt
import plot_tools
import numpy as np
import pandas as pd


# def get_forecasts(model_fit, exog=None, number_past_the_end=10):
#   """Get the forecasts of a SARIMAX model"""
#   forecast = model_fit.get_forecast(steps=number_past_the_end, exog=exog)
#   conf_int = forecast.conf_int()
#   return pd.DataFrame(dict(mean_forecast = forecast.predicted_mean.values,
#                            upper_forecast = conf_int.iloc[:,1].values,
#                            lower_forecast = conf_int.iloc[:,0].values),
#                       index=forecast.row_labels
#                       )


def plot_arima_predictions(predict, label='prediction', ci_label='confidence', ci_alpha=0.3, pred_linestyle='--',
                         pred_color=None, ax=None, ) -> pd.DataFrame:
    """Plot the predictions with confidence intervals"""
    predict_ci = predict.conf_int()
    predict.predicted_mean.plot(ax=ax, label=label, linestyle=pred_linestyle, color=pred_color)
    plot_tools.plot_ci(predict_ci, label=ci_label, ax=ax, alpha=ci_alpha)
    return pd.concat([predict.predicted_mean, predict_ci],axis=1)


def SARIMAX_model(df_data : pd.DataFrame, endog_name, exog_name, order=(30, 1, 10), plot_diagnostics=False, fit_summary=False):
    """A toy function to run a SARIMAX model on the supplied data. Must be supplied with a column called 'type' which
    has values 'forecast" and 'actual' to describe the historic and forecast sections of the data"""
    df_actual = df_data[df_data['type'] == 'actual'].copy()[[endog_name,exog_name]].dropna(how='any')
    endogenous_actual = df_actual[endog_name]
    exogenous_actual = df_actual[exog_name]
    exogenous_forecast = df_data[df_data['type'] == 'forecast'].copy()[exog_name]

    # define the model
    model = SARIMAX(endog=endogenous_actual, exog=exogenous_actual, order=order)

    # fit the model to the data
    model_fit = model.fit();
    if plot_diagnostics is True:
        model_fit.plot_diagnostics()
    if fit_summary is True:
        model_fit.summary()

    # make the "in-sample" predictions - I.e. a sort of 1 day backfit
    predict = model_fit.get_prediction()
    # make the "out-of-sample" predictions - I.e. the future predictions]
    forecast = model_fit.get_forecast(exog=np.atleast_2d(exogenous_forecast).T, steps=len(exogenous_forecast))

    # plot the results
    fig, ax = plt.subplots(figsize=(15, 7))
    df_data[endog_name].plot(ax=ax, marker='x', linewidth=0)  # actual data measured
    plot_arima_predictions(predict, label='insample forecast')  # in sample forecast - the fit of the model the data
    plot_arima_predictions(forecast, label='out-of-sample forecast',ci_label=None)  # out-of-sample-forecast - the forecast of the future
    ax.set_ylim(-70, )
    plt.legend()


