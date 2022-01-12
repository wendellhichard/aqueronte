class Aqueronte:
    # Constructor parametrizado
    def __init__(self, jdbcHostname, jdbcDatabase, jdbcPort, jdbcUsername, jdbcPassword):
        # Atributos da conexao
        self.jdbcHostname = jdbcHostname
        self.jdbcDatabase = jdbcDatabase
        self.jdbcPort = jdbcPort
        self.jdbcUsername = jdbcPassword

        self.jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort,
                                                                                            jdbcDatabase, jdbcUsername,
                                                                                            jdbcPassword)

        self.connectionProperties = {"user": jdbcUsername, "password": jdbcPassword,
                                     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

    # Le uma determinada tabela usando os parametros de conexao declarados
    def read(self, table_name):
        try:
            df = spark.read.jdbc(url=self.jdbcUrl, table=table_name, properties=self.connectionProperties)
            return df
        except:
            print("Something went wrong when reading to the table")

    # Faz o upload de um df para a base usando os parametros de conexao declarados
    def write(self, df, table_name, mode='overwrite'):
        try:
            df.write.mode('overwrite').jdbc(url=self.jdbcUrl, table=table_name, properties=self.connectionProperties)
            return 'Success'
        except:
            print("Something went wrong when writing to the table")