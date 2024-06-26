import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta


class Analytics:
    def __init__(self, db_path, left_side=None, right_side=None, marketplace=None):
        self.conn = sqlite3.connect(db_path)
        self.orders = self.load_data()
        self.filtered_orders = pd.DataFrame(columns=self.orders.columns)
        self.is_period = left_side is not None and right_side is not None
        self.left_side = pd.to_datetime(left_side).date() if left_side else None
        self.right_side = pd.to_datetime(right_side).date() if right_side else None
        self.marketplace = marketplace

    def load_data(self):
        query = """
        SELECT 
            items.order_id, 
            items.item_id, 
            items.item_count, 
            items.cart AS price, 
            items.payment, 
            items.tariff_name, 
            items.tariff_rate, 
            orders.date AS order_date, 
            marketplaces.marketplace_name AS marketplace, 
            orders.is_delivered
        FROM items
        JOIN orders ON items.order_id = orders.order_id
        JOIN marketplaces ON orders.marketplace_id = marketplaces.marketplace_id
        """
        return pd.read_sql(query, self.conn, parse_dates=['order_date'])

    def filter_orders(self, analytics_time_type):
        current_date = datetime.now().date()

        if self.marketplace:
            self.filtered_orders = self.orders[self.orders['marketplace'] == self.marketplace]
        else:
            self.filtered_orders = self.orders

        if analytics_time_type == 'день':
            self.filtered_orders = self.filtered_orders[self.filtered_orders['order_date'].dt.date == current_date]
        elif analytics_time_type == 'неделя':
            week_start = current_date - pd.to_timedelta(current_date.weekday(), unit='D')
            week_end = current_date
            self.filtered_orders = self.filtered_orders[
                (self.filtered_orders['order_date'].dt.date >= week_start) & (self.filtered_orders['order_date'].dt.date <= week_end)]
        elif analytics_time_type == 'месяц':
            month_start = current_date.replace(day=1)
            month_end = current_date
            self.filtered_orders = self.filtered_orders[
                (self.filtered_orders['order_date'].dt.date >= month_start) & (self.filtered_orders['order_date'].dt.date <= month_end)]
        elif analytics_time_type == 'год':
            year_start = current_date.replace(month=1, day=1)
            year_end = current_date
            self.filtered_orders = self.filtered_orders[
                (self.filtered_orders['order_date'].dt.date >= year_start) & (self.filtered_orders['order_date'].dt.date <= year_end)]
        elif analytics_time_type == 'период' and self.is_period:
            self.filtered_orders = self.filtered_orders[
                (self.filtered_orders['order_date'].dt.date >= self.left_side) & (
                        self.filtered_orders['order_date'].dt.date <= self.right_side)]
        else:
            self.filtered_orders = self.filtered_orders

    def total_sales(self):
        total_sales_sum = np.sum(self.filtered_orders['price'] * self.filtered_orders['item_count'])
        total_sales_count = np.sum(self.filtered_orders['item_count'])
        return total_sales_sum, total_sales_count

    def total_sales_without_returns(self):
        non_returned_orders = self.filtered_orders[self.filtered_orders['is_delivered'] == 1]
        total_sales_sum = np.sum(non_returned_orders['price'] * non_returned_orders['item_count'])
        total_sales_count = np.sum(non_returned_orders['item_count'])
        return total_sales_sum, total_sales_count

    def sales_by_marketplace(self):
        temp_filtered_orders = self.filtered_orders.copy()
        temp_filtered_orders['effective_price'] = temp_filtered_orders.apply(
            lambda x: x['price'] * x['item_count'] if x['is_delivered'] == 1 else 0, axis=1
        )
        result = temp_filtered_orders.groupby('marketplace')['effective_price'].sum().reset_index()
        return result

    def sales_by_date(self):
        temp_filtered_orders = self.filtered_orders.copy()
        temp_filtered_orders['effective_price'] = temp_filtered_orders.apply(
            lambda x: x['price'] * x['item_count'] if x['is_delivered'] == 1 else 0, axis=1
        )
        result = temp_filtered_orders.groupby(temp_filtered_orders['order_date'].dt.date)[
            'effective_price'].sum().reset_index()
        return result

    def sales_by_tariff(self):
        result = self.filtered_orders.groupby('tariff_name')['price'].sum().reset_index()
        return result

    def sales_by_category(self):
        result = self.filtered_orders.groupby('tariff_rate')['price'].sum().reset_index()
        return result

    def _split_periods(self, analytics_time_type):
        if self.orders.empty:
            return None

        start_date = self.left_side if self.is_period else self.orders['order_date'].min().date()
        end_date = self.right_side if self.is_period else self.orders['order_date'].max().date()

        if analytics_time_type == 'день':
            periods = pd.date_range(start=start_date, end=end_date, freq='D')
        elif analytics_time_type == 'неделя':
            periods = pd.date_range(start=start_date - timedelta(days=start_date.weekday()), end=end_date, freq='W-MON')
        elif analytics_time_type == 'месяц':
            periods = pd.date_range(start=start_date.replace(day=1), end=end_date, freq='MS')
        elif analytics_time_type == 'год':
            periods = pd.date_range(start=start_date.replace(month=1, day=1), end=end_date, freq='YS')
        elif analytics_time_type == 'период' and self.is_period:
            periods = pd.date_range(start=start_date, end=end_date, freq=self.right_side-self.left_side)
        else:
            return None

        return periods.date

    def _move_end_period(self, analytics_time_type, period_start):
        if analytics_time_type == 'день':
            return period_start
        elif analytics_time_type == 'неделя':
            return period_start + timedelta(days=6)
        elif analytics_time_type == 'месяц':
            return period_start.replace(day=pd.to_datetime(period_start).days_in_month)
        elif analytics_time_type == 'год':
            return period_start.replace(month=12, day=31)
        elif analytics_time_type == 'период' and self.is_period:
            return self.right_side

    def average_sales(self, analytics_time_type):
        periods = self._split_periods(analytics_time_type)

        total_sales_sum = 0
        for period_start in periods:
            period_end = self._move_end_period(analytics_time_type, period_start)

            period_orders = self.orders[(self.orders['order_date'].dt.date >= period_start) &
                                        (self.orders['order_date'].dt.date <= period_end)]

            if self.marketplace:
                period_orders = period_orders[period_orders['marketplace'] == self.marketplace]

            total_sales_sum += period_orders['price'].sum()

        if len(periods) > 0:
            avg_sales_sum = total_sales_sum / len(periods)
        else:
            avg_sales_sum = self.total_sales()[0]

        return avg_sales_sum

    def average_items_sold(self, analytics_time_type):
        periods = self._split_periods(analytics_time_type)

        total_items_count = 0
        for period_start in periods:
            period_end = self._move_end_period(analytics_time_type, period_start)

            period_orders = self.orders[(self.orders['order_date'].dt.date >= period_start) &
                                        (self.orders['order_date'].dt.date <= period_end)]

            if self.marketplace:
                period_orders = period_orders[period_orders['marketplace'] == self.marketplace]

            total_items_count += period_orders['item_count'].sum()

        if len(periods) > 0:
            avg_items_count = total_items_count / len(periods)
        else:
            avg_items_count = self.total_sales()[1]

        return avg_items_count

    def average_sales_without_returns(self, analytics_time_type):
        periods = self._split_periods(analytics_time_type)

        total_sales_sum = 0
        for period_start in periods:
            period_end = self._move_end_period(analytics_time_type, period_start)

            period_orders = self.orders[(self.orders['order_date'].dt.date >= period_start) &
                                        (self.orders['order_date'].dt.date <= period_end) &
                                        (self.orders['is_delivered'] == 1)]

            if self.marketplace:
                period_orders = period_orders[period_orders['marketplace'] == self.marketplace]

            total_sales_sum += period_orders['price'].sum()

        if len(periods) > 0:
            avg_sales_sum = total_sales_sum / len(periods)
        else:
            avg_sales_sum = self.total_sales_without_returns()[0]

        return avg_sales_sum

    def average_items_sold_without_returns(self, analytics_time_type):
        periods = self._split_periods(analytics_time_type)

        total_items_count = 0
        for period_start in periods:
            period_end = self._move_end_period(analytics_time_type, period_start)

            period_orders = self.orders[(self.orders['order_date'].dt.date >= period_start) &
                                        (self.orders['order_date'].dt.date <= period_end) &
                                        (self.orders['is_delivered'] == 1)]

            if self.marketplace:
                period_orders = period_orders[period_orders['marketplace'] == self.marketplace]

            total_items_count += period_orders['item_count'].sum()

        if len(periods) > 0:
            avg_items_count = total_items_count / len(periods)
        else:
            avg_items_count = self.total_sales_without_returns()[1]

        return avg_items_count

    def percentage_change(self, previous_sum, current_sum):
        if previous_sum == 0:
            return None, False
        change = (current_sum - previous_sum) / previous_sum * 100
        return abs(change), change > 0


def count_analytics(marketplace, analytics_time_type, left_side, right_side):
    db_path = '../sovet5.db'
    result = {'error': False}

    analytics = Analytics(db_path, left_side, right_side, marketplace)
    analytics.filter_orders(analytics_time_type)

    total_sales_sum, total_sales_count = analytics.total_sales()
    result['sum'] = {'value': total_sales_sum}
    result['count'] = {'value': total_sales_count}

    avg_sales_sum = analytics.average_sales(analytics_time_type)
    result['sum']['avg'] = avg_sales_sum

    change, is_increase = analytics.percentage_change(avg_sales_sum, total_sales_sum)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        result['sum']['sign'] = sign
    else:
        result['sum']['sign'] = None

    avg_items_sold_sum = analytics.average_items_sold(analytics_time_type)
    result['count']['avg'] = avg_items_sold_sum

    change, is_increase = analytics.percentage_change(avg_items_sold_sum, total_sales_count)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        result['count']['sign'] = sign
    else:
        result['count']['sign'] = None

    total_sales_without_returns_sum, total_sales_without_returns_count = analytics.total_sales_without_returns()
    result['without_returns_sum'] = {'value': total_sales_without_returns_sum}
    result['without_returns_count'] = {'value': total_sales_without_returns_sum}

    avg_sales_without_returns_sum = analytics.average_sales_without_returns(analytics_time_type)
    result['without_returns_sum']['avg'] = total_sales_without_returns_sum

    change, is_increase = analytics.percentage_change(avg_sales_without_returns_sum, total_sales_without_returns_sum)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        result['without_returns_sum']['sign'] = sign
    else:
        result['without_returns_sum']['sign'] = None

    avg_items_without_returns_sold_sum = analytics.average_items_sold_without_returns(analytics_time_type)
    result['without_returns_count']['avg'] = total_sales_without_returns_sum

    change, is_increase = analytics.percentage_change(avg_items_without_returns_sold_sum,
                                                      total_sales_without_returns_count)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        result['without_returns_count']['sign'] = sign
    else:
        result['without_returns_count']['sign'] = None

    sales_by_marketplace = analytics.sales_by_marketplace()
    result['sales_by_marketplace'] = sales_by_marketplace

    sales_by_date = analytics.sales_by_date()
    result['sales_by_date'] = sales_by_date

    sales_by_tariff = analytics.sales_by_tariff()
    result['sales_by_tariff'] = sales_by_tariff

    sales_by_category = analytics.sales_by_category()
    result['sales_by_category'] = sales_by_category

    # print(result)
    return str(result)


def main():
    # Путь к базе данных
    db_path = '../sovet5.db'

    analytics_time_type = input("Аналитика по (день / неделя / месяц / год / период): ")
    left_side, right_side = None, None
    if analytics_time_type in ["день", "неделя", "месяц", "год"]:
        left_side = datetime.now().date()
    elif analytics_time_type == "период":
        left_side = input("Введите левую границу периода (YYYY-MM-DD): ")
        right_side = input("Введите правую границу периода (YYYY-MM-DD): ")
    else:
        raise ValueError("Неверный фильтр аналитики")

    marketplace = input("Введите название маркетплейса для аналитики (или оставьте пустым для всех): ")
    marketplace = marketplace if marketplace else None

    analytics = Analytics(db_path, left_side, right_side, marketplace)
    analytics.filter_orders(analytics_time_type)

    total_sales_sum, total_sales_count = analytics.total_sales()
    print(f'Сумма продаж: {total_sales_sum} RUB, Количество проданных товаров : {total_sales_count}')

    avg_sales_sum = analytics.average_sales(analytics_time_type)
    if avg_sales_sum is not None:
        print(
            f'Средняя продажа на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}: {avg_sales_sum} RUB')
    else:
        print(
            f'Не получилось рассчитать среднюю продажу на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}.')

    change, is_increase = analytics.percentage_change(avg_sales_sum, total_sales_sum)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        print(f'Процентное изменение: {change:.2f}% ({sign})')
    else:
        print("Невозможно рассчитать процентное изменение (деление на ноль).")

    avg_items_sold_sum = analytics.average_items_sold(analytics_time_type)
    if avg_items_sold_sum is not None:
        print(
            f'Среднее количество продаж на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}: {avg_items_sold_sum}')
    else:
        print(
            f'Не получилось рассчитать среднее количество продаж на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}.')

    change, is_increase = analytics.percentage_change(avg_items_sold_sum, total_sales_count)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        print(f'Процентное изменение: {change:.2f}% ({sign})')
    else:
        print("Невозможно рассчитать процентное изменение (деление на ноль).")

    total_sales_without_returns_sum, total_sales_without_returns_count = analytics.total_sales_without_returns()
    print(
        f'Сумма продаж - сумма возвратов: {total_sales_without_returns_sum} RUB, Количество проданных товаров - количество возвращенных товаров: {total_sales_without_returns_count}')

    avg_sales_without_returns_sum = analytics.average_sales_without_returns(analytics_time_type)
    if avg_sales_sum is not None:
        print(
            f'Средняя продажа на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}: {avg_sales_without_returns_sum} RUB')
    else:
        print(
            f'Не получилось рассчитать среднюю продажу на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}.')

    change, is_increase = analytics.percentage_change(avg_sales_without_returns_sum, total_sales_without_returns_sum)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        print(f'Процентное изменение: {change:.2f}% ({sign})')
    else:
        print("Невозможно рассчитать процентное изменение (деление на ноль).")

    avg_items_without_returns_sold_sum = analytics.average_items_sold_without_returns(analytics_time_type)
    if avg_items_sold_sum is not None:
        print(
            f'Среднее количество продаж на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}: {avg_items_without_returns_sold_sum}')
    else:
        print(
            f'Не получилось рассчитать среднее количество продаж на промежутке {analytics.orders["order_date"].min().date()} - {analytics.orders["order_date"].max().date()} '
            f'с периодичностью 1 {analytics_time_type}.')

    change, is_increase = analytics.percentage_change(avg_items_without_returns_sold_sum,
                                                      total_sales_without_returns_count)
    if change is not None:
        sign = "увеличение" if is_increase else "уменьшение"
        sign = "отсутсвует изменение" if avg_sales_sum == total_sales_sum else sign
        print(f'Процентное изменение: {change:.2f}% ({sign})')
    else:
        print("Невозможно рассчитать процентное изменение (деление на ноль).")

    sales_by_marketplace = analytics.sales_by_marketplace()
    print('Продажи по маркетплейсам:')
    print(sales_by_marketplace)

    sales_by_date = analytics.sales_by_date()
    print('Продажи по дням:')
    print(sales_by_date)

    sales_by_tariff = analytics.sales_by_tariff()
    print('Продажи по тарифам:')
    print(sales_by_tariff)

    sales_by_category = analytics.sales_by_category()
    print('Продажи по категориям:')
    print(sales_by_category)


if __name__ == '__main__':
    # main()
    result = count_analytics(None, "год", None, None)
    print(result)