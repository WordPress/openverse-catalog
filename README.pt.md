<img src="https://github.com/WordPress/openverse/raw/main/brand/banner.svg" width="100%"/>

<p align="center">
  <a href="https://github.com/orgs/WordPress/projects/3">Project Board</a> | <a href="https://make.wordpress.org/openverse/">Community Site</a> | <a href="https://make.wordpress.org/chat/">#openverse @ Slack</a> | <a href="https://make.wordpress.org/openverse/handbook/">Handbook</a> | <a href="https://www.figma.com/file/w60dl1XPUvSaRncv1Utmnb/Openverse-Releases">Figma Mockups</a>  | <a href="https://www.figma.com/file/GIIQ4sDbaToCfFQyKMvzr8/Openverse-Design-Library">Figma Design Library</a> | <a href = "https://github.com/WordPress/openverse-catalog/blob/main/README.md">EN Readme</a>
</p>

# Catálogo Openverse

Este repositorio contém os métodos utilizados para identificar cerca de 1.4 bilhões
de trabalhos com licença Creative Commons. O desafio é que esses trabalhos estão
dispersos por toda a web e identifica-los requer uma combinação de técnicas.

Duas abordagens que atualmente estão em uso são:

1. Web crawl data
2. Interface de Programação de Aplicativos (API Data)

## Dados Web Crawl

A organização Common Crawl providencia um repositorio aberto contendo petabytes de
dados percorridos da web. Um novo dataset é publicado a cada fim de mês composto
de cerca de 200 TiB de dados descompactados.

Os dados estão disponiveis em três formatos de arquivo:

- WARC (Web ARChive): Toda a informação crua, incluindo metadata de respostas HTTP,
  metadata de WARC, etc.
- WET: texto sem formatação de cada página.
- WAT: extração de metadata de html, e.g. cabeçalhos e hyperlinks HTTP, etc.

Para mais informações a cerca destes formatos, por favor veja:
[Common Crawl documentation][ccrawl_doc].

O catalogo do openvese utiliza os pipelines de dados da AWS para automaticamente criar
um cluster EMR da amazon de 100 instancias c4.8xlarge que irão percorrer os arquivos
WAT para identificar todos os dominios que linkam ao creativecommons.org. Devido ao
volume de dados, Apache Spark é utilizado para racionalizar o processo. A saída
desta metodologia é uma série de arquivos parquet que possuem:

- Os dominios e seus respectivos caminhos de conteudo e strings de query (i.e.
  a pagina exata que conecta com creativecommons.org)
- O CC que referencia o hypperlink (que pode indicar uma licença)
- HTML meta data em formato JSON que indica o número de imagens em cada
  página e outros dominios que eles referenciam;
- A localização da página web no arquivo WARC de forma que os conteudos da página
  possam ser encontrados

Os passos a seguir são realizados no [`ExtractCCLinks.py`][ex_cc_links].

[ccrawl_doc]: https://commoncrawl.org/the-data/get-started/
[ex_cc_links]: archive/ExtractCCLinks.py

## API Data

[Apache Airflow](https://airflow.apache.org/) é utilizado para gerenciar o workflow
para varios jobs de API ETL que puxam e processam data de um grande numero de APIs
abertas da internet

### API Workflows

Para ver mais informações sobre todos os workflows disponiveis (DAGs) dentro do projeto,
veja [DAGs.md](DAGs.md).

Veja as notas de script da API de cada provedor em seus respectivos [handbook][ov-handbook].

[ov-handbook]: https://make.wordpress.org/openverse/handbook/openverse-handbook/

## Setup de desenvolvimento para Airflow e scripts de API puller

Existe um número de scripts no diretorio
[`openverse_catalog/dags/provider_api_scripts`][api_scripts] eventualmente carregado em um database que será indexado para buscar dentro da API do Openverse.
Eles rodam em um diferente ambiente que a porção PySpark do projeto, portanto possuem  suas próprias dependencias.

Para instruções feitas especificamente para a produção de deployments, veja [DEPLOYMENT.md](DEPLOYMENT.md)

[api_scripts]: openverse_catalog/dags/providers/provider_api_scripts

### Requerimentos

Voce irá precisar de `docker` e `docker-compose` instalados na sua máquina com versões novas o suficiente para utlizar a versão `3` para os arquivos Docker Compose `.yml`.

Você também irá precisar do comando [`just`](https://github.com/casey/just#installation) instalado.

### Setup

Para fazer o setup do ambiente local de python com o hook de pre-commit, rode:

```shell
python3 -m venv venv
source venv/bin/activate
just install
```

O build dos containers será feito quando for inicializado pela primeira vez.
Se você quiser buildar antes disto, rode:

```shell
just build
```

### Ambiente

Para setar as variaveis de ambiente rode:

```shell
just dotenv
```

Isto irá gerar um arquivo `.env` que é utilizado pelos containers.

O arquivo `.env` é dividido em quatro partes:
1. Configurações do Airflow - este pode ser utilizado para puxar varias propriedades do Airflow.
2. Chaves API - configure estas caso você pretenda testar algum dos provedores de API.
3. Conexão/Informações de variavel - provavelmente não será necessario modifica-las para desenvolvimento local, entretanto seus valores devem ser alterados em produção.
4. Outras configurações - misc,
4. Other config - misc. definições de configuração, algumas são uteis para desenvolvimento local.

O arquivo `.env` não precisa ser modificado caso você somente queira rodar os testes.

### Rodando & Testando

Há um [`docker-compose.yml`][dockercompose] providenciado no
[`openverse_catalog`][cc_airflow], portanto, neste diretório rode:

```shell
just up
```

Estes resultam, entre outras coisas, nos seguintes containers:

- `openverse_catalog_webserver_1`
- `openverse_catalog_postgres_1`
- `openverse_catalog_s3_1`

e em algumas configurações de rede de forma que haja comunicação.

- `openverse_catalog_webserver_1` esta rodando Apache Airflow daemon, como também possui algumas ferramentas de desenvolvimento instaladas (e.g., `pytest`)
- `openverse_catalog_postgres_1` esta rodando PostgreSQL, e esta configurado com alguns databases e tabelas para emular o ambiente de produção. Este também providencia um database para o Airflow armazenar seu estado atual.
- O diretorio contem todos os arquivos de módulo (incluindo DAGs, dependencias, e       outras ferramentas) sera criado na pasta `/usr/local/airflow/openverse_catalog` no container `openverse_catalog_webserver_1`. Em produção, somente as pastas DAGs serão montadas, e.g. `/usr/local/airflow/openverse_catalog/dags`.

Os varios serviços poedm ser acessados através destes links:

- Airflow: `localhost:9090` (Tanto o usuário quanto senha padão são `airflow`.)
- Minio Console: `localhost:5011` (O usuario padrão é `test_key` e a senha `test_secret`)
- Postgres: `localhost:5434` (utlizando conector de database)

Nesta fase, você pode rodar os testes via:

```shell
just test

# De forma alternativa, rode todos os testes incluindo os que irão demorar
just test --extended
```

Edições aos arquivos source tambem podem ser feitos em sua maquina local, enquanto os testes poderão ser executados no container com o comando a seguir.

Se você quiser, é possivel logar ao container do webservice utilizando:

```shell
just shell
```

Caso você queira somente rodar um comando do airflow, você pode utlizar o recipiente `airflow`. Argumentos passados ao airflow devem ser citados:

```shell
just airflow "config list"
```
Para seguir os logs do container atual:

```shell
just logs
```

Para começar um [`pgcli` shell](https://www.pgcli.com/) interativo no database, rode:

```shell
just db-shell
```

Caso você queira derrubar os containers, rode:

```shell
just down
```

Para ressetar o DB de teste (excluindo todos os outros databases, schemas e tabelas), rode:

```shell
just down -v
```

`docker colume prune` pode tambem ser util caso você ja tenha derrubado os containers, mas fique atento que isto irá remover todos os volumes associados com os containers parados, não somente os do openverse-catalog.

Para recriar tudo desde o começo, você pode utilizar:

```shell
just recreate
```

[justfile]: justfile
[dockercompose]: docker-compose.yml
[cc_airflow]: openverse_catalog/

## Estrutura do diretório

```text
openverse-catalog
├── .github/                                # Templates para GitHub
├── archive/                                # Arquivos relacionados a ultima implementação do parser Common Crawl
├── docker/                                 # Dockerfiles e arquivos de suporte
│   ├── airflow/                            #   - Workers e imagens do Airflow para o Docker
│   └── local_postgres/                     #   - Imagem do docker para o desenvolvimento do banco Postgres
├── openverse_catalog/                      # Diretorio de código primario
│   ├── dags/                               # Códigos de suporte DAGs & DAG
│   │   ├── common/                         #   - Modulos compartilhados utilizados sobre DAGs
│   │   ├── commoncrawl/                    #   - DAGs & códigos para parse do commoncrawl
│   │   ├── database/                       #   - DAGs relacionadas a ações no database (matview refresh, cleaning, etc.)
│   │   ├── maintenance/                    #   - DAGs relacionadas ao airflow/manutenção de infra
│   │   ├── oauth2/                         #   - DAGs & código para gerenciamento de chaves Oauth2
│   │   ├── providers/                      #   - DAGs & para injeção do provedor
│   │   │   ├── provider_api_scripts/       #       - Códigos de acesso da API especificos para provedores
│   │   │   └── *.py                        #       - Definição de arquivos DAG para os provedores
│   │   └── retired/                        #   - DAGs & códigos que não são mais uteis mas podem ser utilizados como guia no futuro
│   └── templates/                          # Templates para gerar código de novos provedores
└── *                                       # Documentação, arquivos de configuração, requerimentos de projeto
```

## Publicando

A imagem do docker para o catalogo (Airflow) é publicada para o ghcr.io/WordPress/openverse-catalog.

## Contribuindo

Pull requests são bem-vindos! Sinta-se a vontade para nos juntar no [join us on Slack][wp_slack] e discutir o projeto com os engenheiros e membros da comunidade no #openverse

## Reconhecimentos

Openverse, antigamente conhecido como CC Search, foi concebido e construido no [Creative Commons](https://creativecommons.org). Nós agradecemos a eles por seu comprometimento com a comunidade open source, em particular agradecemos aos membros anteriores @ryanmerkley, @janetpkr, @lizadaly, @sebworks, @pa-w, @kgodey, @annatuma, @mathemancer, @aldenstpage, @brenoferreira, and @sclachar, along with their [community of volunteers](https://opensource.creativecommons.org/community/community-team/).

## Licença

- [`LICENSE`](LICENSE) (Expat/[MIT][mit] License)

[mit]: http://www.opensource.org/licenses/MIT "The MIT License | Open Source Initiative"
[wp_slack]: https://make.wordpress.org/chat/
[cc]: https://creativecommons.org
[cc_community]: https://opensource.creativecommons.org/community/community-team/
