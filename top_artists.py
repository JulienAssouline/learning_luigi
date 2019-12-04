import luigi
import luigi.contrib.hdfs
from luigi.contrib.postgres import CopyToTable
import luigi.contrib.spark

import ujson as json
import logging
import random
from collections import defaultdict

from heapq import nlargest


class Streams(luigi.Task):
    """
    Faked version right now, just generates bogus data.
    """
    date = luigi.DateParameter()

    def run(self):
        """
        Generates bogus data and writes it into the :py:meth:`~.Streams.output` target.
        """
        with self.output().open('w') as output:
            for _ in range(1000):
                output.write('{} {} {}\n'.format(
                    random.randint(0, 999),
                    random.randint(0, 999),
                    random.randint(0, 999)))

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in the local file system.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))


class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for input in self.input():
            with input.open("r") as in_file:
                for line in in_file:
                    timestamp, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open("w") as out_file:
            for artist, count in artist_count.items():
                print >> out_file, artist, count

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval)


class StreamsHdfs(Streams):

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))


class AggregateArtistsSpark(luigi.contrib.spark.SparkSubmitTask):
    date_interval = luigi.DateIntervalParameter()

    app = "top_artists_spark.py"
    master = "local[*]"

    def requires(self):
        return [StreamsHdfs(date) for date in self.date_interval]

    def app_options(self):
        return [",".join([p.path for p in self.input()]),
                self.output().path]

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("data/artist_streams_%s.tsv" % self.date_interval)


class Top10Artists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BoolParameter()

    def requires(self):
        if self.use_hadoop:
            return AggregateArtistsSpark(self.date_interval)
        else:
            return AggregateArtists(self.date_interval)

    def run(self):
        top_10 = nlargest(10, self._input_iterator())
        with self.output().open("w") as out_file:
            for streams, artist in top_10:
                print >> out_file, self.date_interval.date_a, self.date_interval.date_b, artist, streams

    def _input_iterator(self):
        with self.input().open("r") as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield int(streams), int(artist)

    def output(self):
        return luigi.LocalTarget("data/top_artists_%s.tsv" % self.date_interval)


class ArtistTopListToDatabase(CopyToTable):
    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BoolParameter()

    host = "localhost"
    database = "postgres"
    user = "postgres"
    password = ""
    table = "top10artists"
    schema = "public"

    columns = [("date_from", "TEXT"),
               ("date_to", "TEXT"),
               ("artist", "TEXT"),
               ("streams", "INT",)]

    def requires(self):
        return Top10Artists(self.date_interval, self.use_hadoop)
