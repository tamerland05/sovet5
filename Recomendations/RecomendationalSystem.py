import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import sqlite3


class SalesDataAnalyzer:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)

    def load_data(self):
        query = """
        SELECT 
            items.item_id, 
            items.item_count, 
            items.cart, 
            items.payment, 
            items.tariff_name, 
            items.tariff_rate, 
            items.item_rate, 
            orders.date, 
            orders.is_delivered, 
            marketplaces.marketplace_name, 
            sellers.seller_name
        FROM items
        JOIN orders ON items.order_id = orders.order_id
        JOIN marketplaces ON orders.marketplace_id = marketplaces.marketplace_id
        JOIN sellers ON orders.seller_id = sellers.seller_id
        """
        return pd.read_sql(query, self.conn)

    def preprocess_data(self, preprocessed_data):
        preprocessed_data["date"] = pd.to_datetime(preprocessed_data["date"])  # Преобразование столбца "date" в формат datetime
        preprocessed_data["day_of_week"] = preprocessed_data["date"].dt.dayofweek  # Добавление столбца с днём недели

        # Преобразование категориальных переменных в фиктивные/индикаторные переменные
        preprocessed_data = pd.get_dummies(preprocessed_data, columns=["tariff_name", "marketplace_name", "seller_name"])
        preprocessed_data.fillna(preprocessed_data.mean(), inplace=True)  # Заполнение пропущенных значений средними

        # Создание целевой переменной 'sale_success' на основе столбца 'payment'
        preprocessed_data["sale_success"] = preprocessed_data["payment"].apply(lambda x: 1 if x > 0 else 0)

        return preprocessed_data

    def train_model(self, X_train, y_train):
        self.model.fit(X_train, y_train)  # Обучение модели на обучающей выборке

    def evaluate_model(self, X_test, y_test):
        y_pred = self.model.predict(X_test)  # Предсказание модели на тестовой выборке

        return accuracy_score(y_test, y_pred)  # Возврат точности модели

    def make_recommendations(self, X_test):
        all_recommendations = []
        for index, row in X_test.iterrows():
            prediction = self.model.predict([row.drop("item_id")])  # Предсказание модели для каждой строки

            # Если предсказание равно 0, добавление рекомендации в список
            if prediction == 0:
                all_recommendations.append(
                    f'Рекомендация для товара {row["item_id"]}: рассмотреть смену маркетплейса.')

        return all_recommendations


if __name__ == "__main__":
    db_path = "sovet5.db"
    analyzer = SalesDataAnalyzer(db_path)

    data = analyzer.load_data()
    data = analyzer.preprocess_data(data)

    # Подготовка данных для обучения модели
    X = data.drop(['item_id', 'date', 'payment', 'sale_success'], axis=1)  # Признаки для обучения
    y = data['sale_success']  # Целевая переменная!!

    # Разделение данных на обучающую и тестовую выборки
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Обучение модели и оценка ее точности
    analyzer.train_model(X_train, y_train)
    accuracy = analyzer.evaluate_model(X_test, y_test)
    print(f'Точность модели: {accuracy}')  # Вывод точности модели

    recommendations = analyzer.make_recommendations(X_test)
    for recommendation in recommendations:
        print(recommendation)
