''' functions for plotting features against target as a part of investigative analysis in early phases of project '''

import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score, mean_absolute_error
import pandas as pd
import preprocess


def old_plot(flabel, feature, tlabel, target, line=True):
    ''' "OLD" is simply a feature vs target plot with a trend line fitted '''
    plt.scatter(feature, target, color='blue', label='Data Points')
    if line:
        # Fit a linear regression line
        coeffs = np.polyfit(feature, target, deg=1)
        trendline = np.poly1d(coeffs)
        # Plot the trend line
        plt.plot(feature, trendline(feature), color='red', linewidth=2, label='Trend Line')
    # Labels and title
    plt.xlabel(flabel)
    plt.ylabel(tlabel)
    plt.title(f'{flabel} v {tlabel}')
    plt.text(x=feature.min(), y=-15,
             s=f'Skewness = {feature.skew():2f}')
    plt.show()


def stats_plot(flabel, feature, tlabel, target):
    ''' "STATS" includes metrics useful for determining relationship viability of features '''

    fig, ax = plt.subplots()

    # Scatter plot
    ax.scatter(feature, target, color='blue', label='Data Points')

    # Fit linear regression line
    coeffs = np.polyfit(feature, target, deg=1)
    trendline = np.poly1d(coeffs)
    preds = trendline(feature)

    # Plot trend line
    ax.plot(feature, preds, color='red', linewidth=2, label='Trend Line')

    # Compute metrics
    r2 = r2_score(target, preds)
    mae = mean_absolute_error(target, preds)
    bias = mae / sum(target)
    corr = np.corrcoef(feature, target)[0, 1]

    # Labels & title
    ax.set_xlabel(flabel)
    ax.set_ylabel(tlabel)
    ax.set_title(f'{flabel} vs {tlabel}')

    # Add text below the chart
    fig.text(
        0.5, 0.01,
        f'Error = {mae:.4f}    Bias = {bias:.4f}    Corr = {corr:.4f}',
        ha='center', va='bottom', fontsize=12
    )

    # Increase bottom margin so text is visible
    plt.subplots_adjust(bottom=0.15)

    plt.show()


