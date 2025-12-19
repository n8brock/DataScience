''' Classical Statistical Method - LinearRegresson '''
''' utilize domain knowledge of the housing market, apply data, resolve colinearity, and test errors '''
import config
from config import *
import featureplots
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from statsmodels.stats.outliers_influence import variance_inflation_factor

# ---------- PARAMS
plots = False
MODEL_NAME = 'LINEAR REG'

# ---------- LOAD SPLIT DATA - FEATURES ENGINEERED & SELECTED
X_train, X_test, y_train, y_test = split_data(preprocess.read())


def train_model(X, y, validate=True):
    # ---------- FEATURE SELECTION - INDUSTRY KNOWLEDGE
    # larger lots implies demand for OYL - use all lots in assessor data (all time), not just current vacant
    # more production builder permits implies more competition with finished, lower-priced homes (economies of scale)
    # pop growth implies more demand for housing - growth across entire range of data
    # median income implies buying power - more is better for OYL
    # price growth implies equity growth, a better candidate for OYL
    # hh presence and bend capture industry history/presence

    X = X[[
        # supply(land) - subdivision/urban - expect larger lots to be better
        'percsublot',
        # supply(comp) - expect more prod builder competition to be worse
        'percprodbuilder',
        # demand - expect higher pop growth to be better
        'popgrowth2010',
        # aff (income) - expect higher income, more buying power
        'medfamincome',
        # aff (capital) - expect more price growth, more equity/capital
        # 'pricegrowth', - too complex - it's an effect, not a cause
        # local reputation & history of competition - bend is high income likely OYL candidates
        'builderfootprint',
        'homecity'
    ]]

    # ---------- BUILD
    model = LinearRegression()

    # ---------- TRAIN
    model.fit(X, y)
    if validate:
        # ---------- VALIDATE
        if plots:
            for f in X.columns:
                featureplots.old_plot(f, X[f], TARGET_COL, y, line=True)
        print('--- PARAMS')
        print('Intercept', round(model.intercept_, 2))
        params = pd.DataFrame(zip(X.columns, model.coef_), columns=['param', 'value'])
        params['value'] = params['value'].astype(float).round(2)
        params.to_csv('params.csv')
        print(params.to_string())
        print('--- COLLINEARITY (Corr Matrix)')
        print(X.corr().to_string())
        # print(X.corr().abs().to_string())
        print('--- MULTI-COLLINEARITY (VIF)')
        vif = pd.DataFrame()
        vif['feature'] = X.columns
        vif['VIF'] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
        vif.set_index('feature', inplace=True)
        print(vif.to_string())

        y_pred = model.predict(X)

        mae = mean_absolute_error(y, y_pred)
        r, n, p = r2_score(y, y_pred), len(X.index), len(X.columns)
        adj_r2 = 1 - (((1 - r) * (n - 1)) / (n - p - 1))
        print(f'\nTraining Accuracy (AdjR2) {adj_r2:.4f}')
        print(f'Validation (Train MAE) {mae:.4f}')
    return model, X, y


# train_model(X_train, y_train)

# ---------- TEST
model, X, y = train_model(X_test, y_test, validate=False)
final_exam(MODEL_NAME, model, X, y)

