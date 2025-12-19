from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import numpy as np
import matplotlib.pyplot as plt
import preprocess

# ---------- CONFIG
# target
MINDATE = '2000-01-01'
MINSALES = 6
BASE_COLS = ['geoid', 'city', 'county', 'state']
TARGET_COL = 'sales'
# train-test split
TEST_SIZE = 0.15


def split_data(data, view=False):
    # df = df.drop(columns=BASE_COLS)
    X = data.drop(columns=TARGET_COL)
    y = data[TARGET_COL]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=100, test_size=TEST_SIZE)
    if view:
        print(f'train {len(X_train)} | test {len(X_test)}')
    return X_train, X_test, y_train, y_test


def final_exam(model_name, model, X_test, y_test):
    print('-----------------------------------')
    print(model_name, 'FINAL EXAM')
    model.fit(X_test, y_test)
    preds = model.predict(X_test)
    bias = np.mean(preds - y_test)
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    var = np.mean(np.var(preds, axis=0))
    print(f'BIAS     {bias:.4f}')
    print(f'MAE      {mae:.4f}')
    print(f'R2       {r2:.4f}')
    print(f'AVG VAR  {var:.4f}')

    plt.style.use('dark_background')
    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, preds, color='#1f77b4', alpha=0.7, edgecolor='w', s=60)
    plt.title(model_name)
    plt.xlabel('Actuals')
    plt.ylabel('Predictions')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
    metrics_text = f'BIAS: {bias:.4f}\nMAE: {mae:.4f}\nR²: {r2:.4f}\nAVG VAR: {avg_var:.4f}'
    plt.text(0.05, 0.95, metrics_text, transform=plt.gca().transAxes,
             fontsize=12, verticalalignment='top', bbox=dict(facecolor='black', alpha=0.5, boxstyle='round,pad=0.5'))
    plt.show()
    print('-----------------------------------')
