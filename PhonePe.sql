CREATE TABLE DINESH.ARR_TRANS_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									TRANS_STATE VARCHAR(255),
                                    TRANS_YEAR INT,
                                    TRANS_QRT INT,
                                    TRANS_TYPE VARCHAR(255),
                                    TRANS_CNT INT,
                                    TRANS_AMT DOUBLE,
                                    PRIMARY KEY (ID),
                                    );
CREATE TABLE DINESH.ARR_USER_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									USER_STATE VARCHAR(255),
                                    USER_YEAR INT,
                                    USER_QRT INT,
                                    USER_DEV VARCHAR(255),
                                    USER_CNT INT,
                                    USER_PER FLOAT,
                                    PRIMARY KEY (ID)
                                    );   

CREATE TABLE DINESH.MAP_TRANS_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									TRANS_STATE VARCHAR(255),
                                    TRANS_YEAR INT,
                                    TRANS_QRT INT,
                                    TRANS_DIST VARCHAR(255),
                                    TRANS_CNT INT,
                                    TRANS_AMT DOUBLE,
                                    PRIMARY KEY (ID)
                                    );

CREATE TABLE DINESH.MAP_USER_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									USER_STATE VARCHAR(255),
                                    USER_YEAR INT,
                                    USER_QRT INT,
                                    USER_DIST VARCHAR(255),
                                    USER_CNT INT,
                                    PRIMARY KEY (ID)
                                    );    	
CREATE TABLE DINESH.TOP_TRANS_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									TRANS_STATE VARCHAR(255),
                                    TRANS_YEAR INT,
                                    TRANS_QRT INT,
                                    TRANS_PIN INT,
                                    TRANS_CNT INT,
                                    TRANS_AMT DOUBLE,
                                    PRIMARY KEY (ID)
                                    );

CREATE TABLE DINESH.TOP_USER_DATA  (ID MEDIUMINT NOT NULL AUTO_INCREMENT,
									USER_STATE VARCHAR(255),
                                    USER_YEAR INT,
                                    USER_QRT INT,
                                    USER_PIN INT,
                                    USER_CNT INT,
                                    PRIMARY KEY (ID)
                                    ); 

CREATE TABLE DINESH.MAP_COORDINATES(ID MEDIUMINT NOT NULL AUTO_INCREMENT,
                                    MAP_STATE   VARCHAR(255),
                                   DATA_STATE   VARCHAR(255),
                                   PRIMARY KEY (ID))  ;                              
  									