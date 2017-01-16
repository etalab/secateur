# Secateur

Dynamic CSV splitting based on columns’ values.


## Installing

Python 3 and [Nameko](http://nameko.readthedocs.io/) with Redis and RabbitMQ.
There is a `requirements.txt` file ready to helps you with your virtualenv.


## Running

```shell
$ redis-server
$ rabbitmq-server
$ nameko run secateur.http
$ nameko run secateur.downloader
$ nameko run secateur.reducer
```

Use the `--config config-debug.yaml` to get some useful log messages.


## API

Submit a new file to split:

```shell
$ http :8000/process url=="https://www.data.gouv.fr/storage/f/2014-03-31T09-49-28/muni-2014-resultats-com-1000-et-plus-t2.txt" column=="Code du département" value=="01" column=="Code de la commune" value=="004"
HTTP/1.1 202 ACCEPTED
...
{
    "hash": "8c5020491f"
}
```

If asking results immediately:

```shell
$ http :8000/status/8c5020491f
HTTP/1.1 404 NOT FOUND
...
```

Once the file has been downloaded:

```shell
$ http :8000/status/8c5020491f
HTTP/1.1 428 PRECONDITION REQUIRED
...
```

Once the file has been reduced:

```shell
$ http :8000/status/8c5020491f
HTTP/1.1 201 CREATED
...
```

Now we can retrieve (and even link to) the resulting file:

```shell
$ http :8000/file/8c5020491f
HTTP/1.1 200 OK
...

Date de l'export;Code du département;Type de scrutin;Libellé du département;Code de la commune;Libellé de la commune;Inscrits;Abstentions;% Abs/Ins;Votants;% Vot/Ins;Blancs et nuls;% BlNuls/Ins;% BlNuls/Vot;Exprimés;% Exp/Ins;% Exp/Vot;Code Nuance;Sexe;Nom;Prénom;Liste;Sièges / Elu;Sièges Secteur;Sièges CC;Voix;% Voix/Ins;% Voix/Exp;
31/03/2014 09:33:41;01;LI2;AIN;004;Ambérieu-en-Bugey;00008198;00003619;44,14;00004579;55,86;00000211;2,57;4,61;00004368;53,28;95,39;LDVG;F;EXPOSITO;Josiane;AMBERIEU AMBITION;3;0;1;00000949;11,58;21,73;LDVG;F;PIDOUX;Catherine;VIVONS NOTRE VILLE;3;0;0;00000741;9,04;16,96;LDVD;M;FABRE;Daniel;PAROLE  AUX AMBARROIS;27;0;7;00002678;32,67;61,31;
```


## To discuss

* store a complete log of requests to be able to replay everything?

