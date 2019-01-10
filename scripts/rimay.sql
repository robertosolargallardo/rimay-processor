create database rimay;
use rimay;
CREATE TABLE rimay.statistics ( id int , timestamp bigint , who text , count bigint , sum bigint , min int , average double , max int, PRIMARY KEY (id, timestamp) );
