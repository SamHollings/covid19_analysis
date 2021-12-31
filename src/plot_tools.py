import matplotlib.pyplot as plt
import pandas as pd

def plot_ci(df_ci: pd.DataFrame,label='confidence intervals',ax=None,alpha=0.3,color='grey'):
    """
    Make confidence interval fill plot on the supplied axes.
    The dataframe must be supplied as follows: index is X, column1 lower values, column2 upper values
    :param df_ci:
    :param label:
    :param ax:
    :param alpha:
    :param color:
    :return: plt object
    """
    plot_params = dict(x=df_ci.index, y1=df_ci.iloc[:,0], y2=df_ci.iloc[:,1],
                       label=label, alpha=alpha,color=color)
    if ax is None:
        return plt.fill_between(**plot_params)
    else:
        return ax.fill_between(**plot_params)
