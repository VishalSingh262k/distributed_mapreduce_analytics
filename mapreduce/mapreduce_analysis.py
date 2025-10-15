# Importing MapReduce framework components
from mrjob.job import MRJob
from mrjob.step import MRStep

# Defining distributed wine chemical analytics job
class DistributedWineFeatureStats(MRJob):

    # Defining pipeline execution stages
    def steps(self):
        return [
            MRStep(
                mapper=self.extract_wine_measurements,
                reducer=self.aggregate_feature_totals
            ),
            MRStep(
                reducer=self.compute_ranked_feature_means
            )
        ]

   
    # Mapper Stage
    def extract_wine_measurements(self, _, raw_record):
        """
        Reading dataset line
        Splitting wine category and feature values
        Emitting feature-level partial totals
        """

        # Splitting CSV record
        record_parts = raw_record.strip().split(",")

        # Extracting wine label
        wine_label = record_parts[0]

        # Converting chemical readings into numeric list
        try:
            chemical_vector = [float(v) for v in record_parts[1:]]
        except ValueError:
            return

        # Emitting feature-wise measurements
        for feature_position, measurement in enumerate(chemical_vector):
            yield (
                (wine_label, feature_position),
                (measurement, 1)
            )

    # First Reduce Stage    
    def aggregate_feature_totals(self, compound_key, measurement_pairs):
        """
        Aggregating totals and counts for each feature
        """

        cumulative_sum = 0
        observation_count = 0

        for measurement, count in measurement_pairs:
            cumulative_sum += measurement
            observation_count += count

        wine_label, feature_position = compound_key

        yield wine_label, (
            feature_position,
            cumulative_sum,
            observation_count
        )

    # Final Reduce Stage
    def compute_ranked_feature_means(self, wine_label, feature_stats):
        """
        Calculating averages
        Ranking features
        Returning top dominant chemical signals
        """

        feature_average_list = []

        for feature_position, total_sum, total_count in feature_stats:
            if total_count > 0:
                mean_value = total_sum / total_count
                feature_average_list.append(
                    (feature_position, mean_value)
                )

        # Ranking features descending
        feature_average_list.sort(
            key=lambda x: x[1],
            reverse=True
        )

        # Output header
        yield wine_label, {
            "column": "Feature_Index",
            "metric": "Mean_Concentration"
        }

        # Output top influential chemical properties
        for feature_position, avg_val in feature_average_list[:13]:
            yield wine_label, {
                "feature_index": feature_position,
                "average_value": round(avg_val, 3)
            }



# Job Execution Entry Point
if __name__ == "__main__":
    DistributedWineFeatureStats.run()
