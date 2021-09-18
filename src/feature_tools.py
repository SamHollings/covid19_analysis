import dataclasses


class DataFeature(object):
    """This class will contain a feature and will the be the output of a feature pipeline.

    "Features" are a set attributes which describe some subject, so the dates of birth (feature) of a cohort of patients (subject)"""
    def __init__(self, subject: str, feature_list: list, output_format: str = 'pandas dataframe'):
        """
        Parameters
        ----------
        subject: str
            thing which the features describe, i.e. patient NHS Number
        feature_list
            the list of attributes described about the subject in this "Feature" class object.
        output_format
            the format of the output - allows for compatibility.
        """
        self.subject = subject
        self.feature_list = feature_list
        self.output_format = output_format
        self.feature_data = None
        self.feature_pipeline = None

        #ToDo: How do we handle running of the Feature -> will this be triggered by something else? Perhaps need a method to do that, which will call the run method of the pipeline

    def update(self):
        """Tells the feature to update itself with the latest data, via the defined feature pipeline."""
        self.feature_data = self.feature_pipeline.run()


class FeaturePipeline(object):
    """This class wraps the feature pipeline function, and allows for the source and destination to be defined."""
    def __init__(self, feature_function, source: str = 'local', source_path: str = '', conn=None):
        """
        Parameters
        ----------
        feature_function: function
            This is the actual function which this pipeline will run - this class is just a wrapper around this function so we can encode the source
        source: str
            The source from which the data will be pulled, i.e. 'local' or 'spark'
        source_path: str
            The 'path' to the data, i.e. for local it will be a filepath - "./data/test.csv"
        conn
            In the case of database sources, this is the connection object, i.e. the spark connection for a spark source
        """
        self.source = source
        self.source_path = source_path
        self.conn = conn
        self.feature_function = feature_function

    def run(self):
        """runs the feature function with the specified source, outputting the specfied feature data object"""
        pass