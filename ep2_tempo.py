from datetime import datetime
import time

from mrjob.job import MRJob
from mrjob.step import MRStep

class EP2_tempo(MRJob):
  

    def mapper_ep2(self, chave, valor):
        dt1 = valor.split(",")[1]
        dt2 = valor.split(",")[2]
        valor = valor.split(",")[16]

        if dt1 != "tpep_pickup_datetime":
            dt_inicio = datetime.strptime(dt1, '%Y-%m-%d %H:%M:%S')
            dt_fim = datetime.strptime(dt2, '%Y-%m-%d %H:%M:%S')

            # Convert to Unix timestamp
            d1_ts = time.mktime(dt_inicio.timetuple())
            d2_ts = time.mktime(dt_fim.timetuple())

            vr_chave = [d1_ts , d2_ts]
            yield vr_chave, float(valor)

    def mapper_ep2_tempo(self, chave, valor):
        dt1 = chave[0]
        dt2 = chave[1]
        tempo_corrida =(dt2-dt1) /60
        yield tempo_corrida, valor

    def reducer2(self, chave, valores):
        yield chave, sum(valores)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_ep2,
            #MRStep(mapper=self.mapper_ep2_tempo,
                   reducer=self.reducer2),
        ]



if __name__ == '__main__':
    EP2_tempo.run()



#vr_chave = [d1_ts , d2_ts]
#ts = int(d1_ts)
#yield datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'), float(valor)
