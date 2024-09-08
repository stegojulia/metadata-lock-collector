from aggregate_locks import aggregate_locks


def run_all_scenarios():
    aggregate_locks(
        name="insert-addcolumn-insert",
        query_1="INSERT INTO `test_table` (id,val) VALUES (5, 'anteater');",
        query_2="ALTER TABLE `test_table` ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;",
        query_3="INSERT INTO `test_table` (id,val) VALUES (6, 'armadillo');",
    )

    aggregate_locks(
        name="update-addcolumn-update",
        query_1="UPDATE `test_table` set val = 'panda' where id = 3;",
        query_2="ALTER TABLE `test_table` ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;",
        query_3="UPDATE `test_table` set val = 'porcupine' where id = 2;",
    )

    aggregate_locks(
        name="update-createtable-update",
        query_1="UPDATE `test_table` set val = 'panda' where id = 3;",
        query_2="""CREATE TABLE `child` 
                (`id` int NOT NULL AUTO_INCREMENT,
                `test_table_id` int DEFAULT NULL,
                `val` varchar(10) DEFAULT NULL,
                PRIMARY KEY (`id`),
                CONSTRAINT `fk_test_table` FOREIGN KEY (`test_table_id`)
                REFERENCES `test_table` (`id`)
                ON DELETE CASCADE ON UPDATE NO ACTION );""",
        query_3="UPDATE `test_table` set val = 'porcupine' where id = 2;",
    )

    aggregate_locks(
        name="insert-createtable-insert",
        query_1="INSERT INTO `test_table` (id,val) VALUES (5, 'anteater');",
        query_2="""CREATE TABLE `child` 
                (`id` int NOT NULL AUTO_INCREMENT,
                `test_table_id` int DEFAULT NULL,
                `val` varchar(10) DEFAULT NULL,
                PRIMARY KEY (`id`),
                CONSTRAINT `fk_test_table` FOREIGN KEY (`test_table_id`)
                REFERENCES `test_table` (`id`)
                ON DELETE CASCADE ON UPDATE NO ACTION );""",
        query_3="INSERT INTO `test_table` (id,val) VALUES (6, 'armadillo');"
    )


if __name__ == "__main__":
    run_all_scenarios()
