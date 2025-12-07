def mapper_linear_regression(data_points):
    for x, y in data_points:
        yield 'n', 1
        yield 'sum_x', x
        yield 'sum_y', y
        yield 'sum_xx', x * x
        yield 'sum_xy', x * y

def group_by_key(mapped_data):
    grouped = {}
    for key, value in mapped_data:
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(value)
    return grouped

def reducer_linear_regression(grouped_data):
    # Суммируем статистики
    n = sum(grouped_data.get('n', [0]))
    sum_x = sum(grouped_data.get('sum_x', [0]))
    sum_y = sum(grouped_data.get('sum_y', [0]))
    sum_xx = sum(grouped_data.get('sum_xx', [0]))
    sum_xy = sum(grouped_data.get('sum_xy', [0]))

    # Решаем систему уравнений
    determinant = n * sum_xx - sum_x * sum_x

    if abs(determinant) < 1e-10:
        return None

    w0 = (sum_y * sum_xx - sum_x * sum_xy) / determinant
    w1 = (n * sum_xy - sum_x * sum_y) / determinant

    return w0, w1

def predict(x, coefficients):
    if coefficients is None:
        return None
    w0, w1 = coefficients
    return w0 + w1 * x

# Тестирование
if __name__ == "__main__":
    # Тестовые данные
    data_points = [
        (1, 5.1), (2, 8.2), (3, 10.8), (4, 13.9), (5, 17.1),
        (6, 19.8), (7, 23.2), (8, 25.9), (9, 29.1), (10, 32.0)
    ]

    # MapReduce pipeline
    mapped_data = list(mapper_linear_regression(data_points))
    grouped_data = group_by_key(mapped_data)
    coefficients = reducer_linear_regression(grouped_data)

    if coefficients:
        w0, w1 = coefficients
        print(f"Уравнение: y = {w0:.4f} + {w1:.4f} * x")

        # Тест предсказаний
        test_points = [2.5, 5.5, 8.5]
        for x in test_points:
            y_pred = predict(x, coefficients)
            print(f"x = {x}, y_pred = {y_pred:.4f}")
