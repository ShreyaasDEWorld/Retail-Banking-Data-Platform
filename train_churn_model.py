import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score

# 1️⃣ Load dataset
df = pd.read_csv("ml_training_dataset.csv")

print("Dataset shape:", df.shape)

# 2️⃣ Drop non-feature columns
drop_cols = [
    "customer_id",
    "snapshot_month"
]

df = df.drop(columns=drop_cols)

# 3️⃣ Handle missing values
df = df.fillna(0)

# 4️⃣ Separate features and target
X = df.drop("churn_next_90_flag", axis=1)
y = df["churn_next_90_flag"]

# 5️⃣ Train-test split (time-based would be better later)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

# 6️⃣ Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 7️⃣ Train model
model = LogisticRegression(max_iter=1000)
model.fit(X_train_scaled, y_train)

# 8️⃣ Predictions
y_pred = model.predict(X_test_scaled)
y_prob = model.predict_proba(X_test_scaled)[:, 1]

# 9️⃣ Evaluation
print("\nClassification Report:\n")
print(classification_report(y_test, y_pred))

print("ROC-AUC Score:", roc_auc_score(y_test, y_prob))