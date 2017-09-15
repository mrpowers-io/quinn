class HelloWorld:

    def hello(self):
        return "greetings matthew"

    def with_age_plus_two(self, df):
        return df.withColumn('age_plus_two', df.age + 2)

