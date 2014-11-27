drop table people;
CREATE TABLE IF NOT EXISTS `people` (
  `name` varchar(14) NOT NULL DEFAULT '',
  `age` int(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='';

INSERT INTO `people` VALUES('chris',20),('asia',22),('manda',19),('joy',10),('cici',15),('pypy',30)