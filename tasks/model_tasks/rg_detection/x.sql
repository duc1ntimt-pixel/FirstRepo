-- Xoá bảng cũ nếu đã tồn tại
IF OBJECT_ID('dbo.training_rg_data', 'U') IS NOT NULL
    DROP TABLE dbo.training_rg_data;

-- Tạo bảng tạm cho training
CREATE TABLE dbo.training_rg_data (
    user_id BIGINT PRIMARY KEY,
    birth_year INT,
    registration_date DATETIME2,
    feature1 FLOAT,
    feature2 FLOAT,
    rg INT
);

-- Thêm dữ liệu demo
INSERT INTO dbo.training_rg_data (user_id, birth_year, registration_date, feature1, feature2, rg)
VALUES
(1, 1990, '2023-01-01', 0.5, 1.2, 1),
(2, 1985, '2022-06-15', 0.2, 0.7, 0),
(3, 2000, '2023-07-20', 0.9, 1.0, 1),
(4, 1995, '2023-03-10', 0.3, 0.4, 0),
(5, 1988, '2023-02-01', 0.6, 1.1, 1),
(6, 1992, '2023-04-05', 0.1, 0.5, 0),
(7, 1999, '2023-08-10', 0.7, 1.3, 1),
(8, 1993, '2022-12-20', 0.4, 0.6, 0),
(9, 1991, '2023-05-15', 0.8, 1.0, 1),
(10, 1987, '2023-01-30', 0.2, 0.3, 0);

sqlcmd -S 192.168.100.152,32336 -U sa -P YourStrong!Passw0rd

