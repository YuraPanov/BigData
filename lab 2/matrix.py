def mapper_matrixA(matrix):
    for i in range(len(matrix)):
        for j in range(len(matrix[0])):
            yield [[matrix[i][j]], [i, j]]

def mapper_matrixB(matrix):
    for i in range(len(matrix)):
        for j in range(len(matrix[0])):
            yield [[matrix[i][j]], [i, j]]

def matches_by_k(a_map, b_map):
    match = []
    for val_a, (i, k) in a_map:           # Элемент A[i][k]
        for val_b, (k2, j) in b_map:      # Элемент B[k][j]

            if k == k2:
                match.append((i, j, val_a[0], val_b[0]))
    return match

def reducer(a_map, b_map):
    n_rows_a = max(coord[0] for _, coord in a_map) + 1
    n_cols_b = max(coord[1] for _, coord in b_map) + 1

    C = []
    for row in range(n_rows_a):  # Для каждой строки (0..n_rows_a-1)
        new_row = []
        for col in range(n_cols_b):  # Для каждого столбца (0..n_cols_b-1)
            new_row.append(0)  # Добавляем 0
        C.append(new_row)  # Добавляем строку в матрицу

    for i, j, a, b in matches_by_k(a_map, b_map):
        C[i][j] += a * b
    return C
#
# A = [[1, 2], [3, 4]]
# B = [[5, 6], [7, 8]]

A = [[1, 2, 3],
     [4, 5, 6],
     [7, 8 , 9]]

B = [[7, 8, 9],
     [11,12,13],
     [15,16,17]]

a_map = list(mapper_matrixA(A))
b_map = list(mapper_matrixB(B))
C = reducer(a_map, b_map)
# print(matches_by_k(a_map,b_map))
print(C)
