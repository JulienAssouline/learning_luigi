import luigi


class HelloWorld(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("helloworld.txt")

    def run(self):
        with self.output().open("w") as outfile:
            outfile.write("Hello World!\n")


class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget(self.input().path + ".name_" + self.name)

    def run(self):
        with self.input().open() as infile, self.output().open("w") as outfile:
            text = infile.read()
            text = text.replace("World", self.name)
            outfile.write(text)


if __name__ == "__main__":
    luigi.run()
