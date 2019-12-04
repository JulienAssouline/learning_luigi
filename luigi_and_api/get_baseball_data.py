import luigi
import requests
import ujson as json
import csv


class GetPlayByPlay(luigi.Task):

    url = "https://statsapi.mlb.com/api/v1/game/448671/playByPlay"

    playEvents = []

    def run(self):
        response = requests.get(self.url)
        allPlays = response.json()["allPlays"]
        playEvents = []
        for i in allPlays:
            for n in i["playEvents"]:
                playEvents.append(n)

        print(playEvents)
        with self.output().open("w") as f:
            json.dump(response.json(), f, sort_keys=True, indent=4)

    def output(self):
        filename = "PlayByPlay_448671.json"
        return luigi.LocalTarget(filename)
