import quinn


class TestScalaToPyspark:

    def test_clean_args(self):
        s = quinn.ScalaToPyspark("blah")
        assert s.clean_args("df: DataFrame") == "df"
        assert s.clean_args("df: DataFrame, s1: String") == "df, s1"
        assert s.clean_args("") == ""
