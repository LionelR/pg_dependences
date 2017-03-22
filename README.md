pg_dependences
==============


For a PostgreSQL object (table, view or function), this tool can:

* __Generate a summary__ report of how many dependents objects are linked to
this object. This include all linked objects calling this object (in the
view or function definition), and all tables having some foreign keys to
this object.
* __Generate a graph__ (based on GraphViz, so it's a requirement for this
package) showing all linked objects, in a cascaded way, usefull when you
have to deal with a table modification which can have some impact on a
view, which is used by a view, which in turn is also used by another view,
 and so on...


### Requirements
Packages: Look at the requirements.txt file. So simple...

Python: Python3 is great (not tested with 2)


### Installation

This package relies on GraphViz (http://graphviz.org) to generate graphs, so you have to install it first.
On Debian/Ubuntu, it's simple as

```
apt-get install graphviz
```

After that, you can download the source code and install it

```
git clone https://github.com/LionelR/pg_dependences.git
cd pg_dependences
python setup.py install
```

... or ask Deep Thought to install it with:

```
pip install git+https://github.com/LionelR/pg_dependences.git
```

These commands normally will create a executable on your system (thanks to the great Click package).


### Usage

For help, call help (or better call Saul)

```
pg_dependences.py --help
```

<pre>
Usage: pg_dependences.py [OPTIONS] SCHEMA

  In a defined schema, reports for each table or view the counts of his
  dependents objects (views and functions calling it)  and his foreign keys.
  Can also, for a particular table or view, reports in a cascaded way all
  his dependents objects and his foreign keys, and graph them.

Options:
  -u, --user TEXT      Database user name. Default current user
  -P, --password TEXT  User password. Will be prompted if not set
  -h, --host TEXT      Database host address. Default localhost
  -d, --database TEXT  Database name. Default current user name
  -p, --port INTEGER   Database port to connect to. Default 5432
  -t, --table TEXT     Generate a detailled cascading graph of all objects
                       related to this table or view
  -g, --graph          Graph mode. Only relevant with the --table option
  -o, --output TEXT    Directory where to put the graph file. Default to home
                       directory
  -f, --format TEXT    Graph file format (see Graphviz docs for more infos).
                       The final filename will be formated like
                       schema.table.format. Default to pdf
  --help               Show this message and exit.
</pre>

Getting a summary of dependents objects and foreign keys counts for all tables and views in a schema:

```
pg_dependences residentiel
```

You'll be asked for the database password if not set on the command line, like for the user name if no one is given and the 'USER' environnement variable is empty.

<pre>
Schema       Type        Name                                      Dependents (first level)    Foreign keys
-----------  ----------  --------------------------------------  --------------------------  --------------
residentiel  BASE TABLE  achl                                                             2               1
residentiel  BASE TABLE  achl_groupe                                                      1               4
residentiel  BASE TABLE  bois_repartition_par_modele_equipement                           1               0
residentiel  BASE TABLE  bois_repartition_par_type_equipement                             1               0
residentiel  BASE TABLE  brutes                                                           1               0
residentiel  BASE TABLE  catl                                                             2               1
residentiel  BASE TABLE  catl_groupe                                                      2               4
residentiel  BASE TABLE  chau                                                             0               1

</pre>

For a particular object in the schema, we can have a look in a cascaded way of his dependents (and so the dependents of the dependents, and so on...).

```
pg_dependences -t achl_groupe residentiel
```

<pre>
Type        Name                              Dep./For. Type    Dep./For. object                  Foreign keys
----------  --------------------------------  ----------------  --------------------------------  --------------
BASE TABLE  residentiel.achl_groupe           VIEW              residentiel.vue1_detail_logement
VIEW        residentiel.vue1_detail_logement  FUNCTION          residentiel.create_tc_logements
                                              VIEW              residentiel.verif_tc_logements
BASE TABLE  residentiel.achl_groupe           BASE TABLE        residentiel.achl                  groupe
                                              BASE TABLE        residentiel.tc_conso              achl
                                              BASE TABLE        residentiel.tc_conso_corrigees    achl
                                              BASE TABLE        residentiel.tc_emi                achl

</pre>

And for generating a cascaded graph:

```
pg_dependences -g -t achl_groupe residentiel
```

 It will be saved under your home directory (can be changed with the `-o` option), and the file named like "schema.table.format" (format=pdf by default, can be changed with the -f option).

![Example graph](examples/residentiel.achl_groupe.png?raw=true)

Graph legend:
<table>
<tr>
<th>object</th>
<th>attribute</th>
</tr>

<tr>
<td>table</td>
<td>color:white, border:black</td>
</tr>

<tr>
<td>view</td>
<td>color:light-grey</td>
</tr>

<tr>
<td>function</td>
<td>color:light-blue</td>
</tr>

<tr>
<td>foreign keys columns</td>
<td>on edge</td>
</tr>
</table>