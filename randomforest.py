''' Modern Machine Learning Approach - RandomForestRegressor '''
''' build model, perform permutaiton importance to determine otpimal feature selection through process of elimination, tune and retrain  model on final feature selection, test '''
import time
from randomforestcols import final_cols
import preprocess
from config import *
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.ensemble import RandomForestRegressor

# ---------- PARAMS
NUM_TREES = 200
JOBS = 3
PERMUTATIONS = 10
MODEL_NAME = 'RANDOM FOREST REG'

# ---------- LOAD SPLIT DATA - ENRICHED
enriched = preprocess.read(enriched=True)
X_train, X_test, y_train, y_test = split_data(enriched)


def train_model(X, y, validate=True):
    # ---------- BUILD
    model = RandomForestRegressor(n_estimators=NUM_TREES, n_jobs=JOBS, oob_score=True, random_state=100)

    # ---------- TRAIN
    model.fit(X, y)
    # print(f'Training Accuracy {model.score(X, y):.4f}')
    # print(f'Validation (OOB) {model.oob_score_:.4f}')

    # ---------- FEATURE SELECTION - PERMUTATION IMPORTANCE
    # process of elimination - eliminate negatively correlated features, refit, repeat... until all features contribute positively to prediciton outcomes
    if validate:
        while True:
            print('processing permutation importance...')
            start = time.time()
            i = permutation_importance(model, X, y, n_repeats=PERMUTATIONS, n_jobs=JOBS)
            sorted_i = i.importances_mean.argsort()
            i_data = pd.DataFrame({
                'Feature': X.columns.values[sorted_i],
                'Importance': i.importances_mean[sorted_i]
            })
            print(f'complete! {((time.time() - start) / 60):.4f}min')

            losers = i_data.query('Importance <= 0')['Feature'].tolist()

            # ---------- VALIDATE
            if len(losers) > 0:
                X = X.drop(columns=losers)
                model.fit(X, y)
                print(f'Training Accuracy {model.score(X, y):.4f}')
                print(f'Validation (OOB) {model.oob_score_:.4f}')
            else:
                print('--- FEATURE IMPORTANCE')
                print(i_data.sort_values('Importance', ascending=False).to_string())
                break
    else:
        X = X[final_cols]

    return model, X, y


# train_model(X_train, y_train)

# ---------- TEST
model, X, y = train_model(X_test, y_test, validate=False)
final_exam(MODEL_NAME, model, X, y)


