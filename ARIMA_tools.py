from statsmodels.tsa.statespace.sarimax import SARIMAX
import statsmodels.api as sm
import matplotlib.pyplot as plt
import plot_tools
import numpy as np
import pandas as pd


def get_forecasts(model_fit, exog=None, number_past_the_end=10):
  """Get the forecasts of a SARIMAX model"""
  forecast = model_fit.get_forecast(steps=number_past_the_end, exog=exog)
  conf_int = forecast.conf_int()
  return pd.DataFrame(dict(mean_forecast = forecast.predicted_mean.values,
                           upper_forecast = conf_int.iloc[:,1].values,
                           lower_forecast = conf_int.iloc[:,0].values),
                      index=forecast.row_labels
                      )


def SARIMAX_model(df_data : pd.DataFrame, endog_name, exog_name, order=(30, 1, 10), plot_diagnostics=False, fit_summary=False):
    df_actual = df_data[df_data['type'] == 'actual'].copy().dropna(how='any')
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
    predict_ci = predict.conf_int()
    # make the "out-of-sample" predictions - I.e. the future predictions
    df_forecast = get_forecasts(model_fit, exog=np.atleast_2d(exogenous_forecast).T,
                                number_past_the_end=len(exogenous_forecast))

    fig, ax = plt.subplots(figsize=(15, 7))
    predict.predicted_mean.plot(color='red',ax=ax)
    # plt.fill_between(predict_ci.index, predict_ci[f'lower {endog_name}]'], predict_ci[f'upper {endog_name}'],
    #                  alpha=0.5)
    plot_tools.plot_ci(predict_ci[[f'lower {endog_name}]', f'upper {endog_name}']], label='insample confidence', ax=ax)
    df_forecast['mean_forecast'].plot(ax=ax)
    plt.fill_between(df_forecast.index, df_forecast['lower_forecast'], df_forecast['upper_forecast'], alpha=0.5)
    ax.set_ylim(-70, )

    fig, ax = plt.subplots(figsize=(15, 7))
    # plot actuals
    df_data['covidOccupiedMVBeds'].plot(ax=ax, marker='x', linewidth=0)
    # plot in sample forecasts
    predict.predicted_mean.plot(color='red', ax=ax, label='insample forecast', linestyle='--')
    plot_tools.plot_ci(predict_ci[[f'lower {endog_name}]', f'upper {endog_name}']], label='confidence', ax=ax,alpha=0.3)
    # plot out of sample forecasts
    df_forecast['mean_forecast'].rename('out-of-sample forecast').plot(ax=ax, linestyle='--')
    plot_tools.plot_ci(df_forecast[['lower_forecast', 'upper_forecast']], label=None, ax=ax, alpha=0.3)
    ax.set_ylim(-70, )
    plt.legend()


