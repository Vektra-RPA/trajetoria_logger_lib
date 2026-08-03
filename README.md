# Biblioteca de Logs Trajetória (`trajetoria_logger`)

Logging para RPAs com **garantia de entrega** (R1) e **kill switch** (R2).

A lib grava cada log num buffer local (SQLite) **antes de qualquer rede** e um worker interno
drena esse buffer enviando lotes para a nossa API por HTTPS. Um log só sai do buffer quando a
API **confirma** aquele `log_id`.

```
seu RPA → trajetoria_logger (buffer SQLite) ──HTTPS POST /logs/batch──▶ API Trajetória
                                                                          │
                                                                    RabbitMQ → PostgreSQL
```

> **A lib fala com um único destino: a nossa API.** Ela não conhece RabbitMQ, não recebe
> credencial de infraestrutura e não abre porta nenhuma. Também **não lê `.env` nem variáveis
> de ambiente** — quem decide de onde vem a configuração é a sua aplicação, que a passa
> explicitamente no construtor.

---

## Instalação

```bash
pip install "git+https://github.com/Vektra-RPA/trajetoria_logger_lib.git@v1.1.1"
```

HTTPS **não pede credencial** — o repositório é público para leitura. O `@v1.1.1` fixa a
versão. Requer `git` instalado: o pip clona o repositório e monta o pacote na máquina.

> **Não use `git+ssh://`.** SSH exige chave autorizada na organização mesmo com o repositório
> público — quem pede a chave é o protocolo, não a permissão do repo. Em máquina de cliente
> isso falha com `Permission denied (publickey)`.

Se a máquina não tem `git`, ou não tem saída para o GitHub, instale pelo wheel. Ele é
`py3-none-any`, então o mesmo arquivo serve Windows e Linux:

```bash
pip install ./trajetoria_logger-1.1.1-py3-none-any.whl
```

Gere o wheel com `python -m build --wheel` (sai em `dist/`). Para uma máquina que também não
alcança o PyPI, junte no mesmo diretório os wheels de `requests`, `urllib3`, `idna`, `certifi`
e `charset_normalizer` (`pip download requests`) e instale sem rede:

```bash
pip install --no-index --find-links ./dist requests trajetoria-logger
```

Ou, a partir de um clone local:

```bash
pip install .
```

Única dependência de runtime: `requests`.

---

## Configuração no início da aplicação

Esta é a parte que importa acertar. São cinco passos, na ordem.

### 1. A chave de API vem de fora do código

Cada robô tem a sua chave, emitida por nós. Ela identifica o robô: o `robot_id` gravado em
cada log é **derivado da chave no servidor**, não enviado pela lib. Nunca escreva a chave no
código-fonte.

```python
import os

API_KEY = os.environ["RPA_API_KEY"]          # falha alto e cedo se não estiver configurada
API_URL = os.environ.get("API_BASE_URL", "https://logs.suaempresa.com")
```

### 2. Escolha o caminho do buffer com cuidado

O buffer é um arquivo SQLite que **precisa sobreviver entre execuções** — é ele que segura os
logs quando a rede cai, e o que os envia na próxima execução do robô.

```python
BUFFER = r"C:\ProgramData\Trajetoria\logs\meu_robo.db"
```

Três regras:

- **Caminho absoluto.** O default (`./rpa_buffer.db`) depende do diretório de trabalho; se o
  robô for iniciado de outro lugar, ele não acha o buffer da execução anterior e aquele
  backlog fica órfão no disco.
- **Um arquivo por robô/chave.** Dois robôs apontando para o mesmo buffer é erro: o worker
  drena *tudo* o que encontra usando a *sua* chave, então os logs de um robô seriam gravados
  em nome do outro.
- **Diretório com permissão de escrita** e que não seja limpo por rotina de manutenção.

### 3. Monte a config e instancie — o construtor valida a licença

```python
from trajetoria_logger import RPALicenseError, RPALoggerClient, RPALoggerConfig

config = RPALoggerConfig(
    api_key=API_KEY,
    api_base_url=API_URL,
    buffer_path=BUFFER,
    on_warning=lambda msg: print(f"[logger] {msg}"),   # a lib nunca imprime sozinha
)

try:
    logger = RPALoggerClient(config)
except RPALicenseError as exc:
    # Só dois casos chegam aqui:
    #   1. a API respondeu EXPLICITAMENTE que a chave está inativa;
    #   2. não houve resposta E o período de graça já venceu.
    # Instabilidade de rede com graça válida NÃO cai aqui — o robô inicia normalmente.
    raise SystemExit(f"Execução abortada: {exc}")
```

Se o construtor levantar, nenhum recurso fica aberto — ele fecha buffer e sessão HTTP antes
de propagar.

### 4. Emita logs e ponha checkpoints entre as etapas

`log()` grava no buffer e retorna. **Não há I/O de rede na sua thread** — pode chamar no
caminho quente sem medo.

```python
logger.info("robô iniciou", task="faturamento")   # kwargs viram o campo `context`

for i, item in enumerate(itens, 1):
    logger.raise_if_inactive()                    # checkpoint do kill switch
    logger.debug("processando item", item_id=item.id, posicao=i)
    ...
```

`raise_if_inactive()` é o que torna a parada **cooperativa**: a lib nunca chama `sys.exit()`
nem mata sua thread. Ela só marca o estado; quem decide parar é você, ao chamar o checkpoint.
Coloque-o **entre etapas** — num ponto onde interromper deixa o trabalho consistente.

### 5. Encerre com `close()`, sempre num `finally`

```python
finally:
    restantes = logger.close()     # drena o buffer (até 10s) e para o worker
    if restantes:
        print(f"{restantes} logs não confirmados — sobem na próxima execução")
```

`close()` devolve quantos logs continuam **não confirmados**. Diferente de zero **não é
perda**: eles ficam no arquivo do buffer e vão na próxima execução, pela fila de backlog.

### O bloco completo

```python
import os

from trajetoria_logger import RPALicenseError, RPALoggerClient, RPALoggerConfig

config = RPALoggerConfig(
    api_key=os.environ["RPA_API_KEY"],
    api_base_url=os.environ.get("API_BASE_URL", "https://logs.suaempresa.com"),
    buffer_path=r"C:\ProgramData\Trajetoria\logs\meu_robo.db",
    on_warning=lambda msg: print(f"[logger] {msg}"),
)

try:
    logger = RPALoggerClient(config)
except RPALicenseError as exc:
    raise SystemExit(f"Execução abortada: {exc}")

print(f"execução {logger.execution_id} (robot_id={logger.robot_id})")

try:
    logger.info("robô iniciou", task="faturamento")

    for etapa in etapas:
        logger.raise_if_inactive()
        logger.info("etapa iniciada", etapa=etapa.nome)
        etapa.executar()

    logger.info("robô finalizou com sucesso")

except RPALicenseError as exc:
    # Kill switch acionado NO MEIO da execução: parada limpa, não é falha do robô.
    print(f"parada cooperativa: {exc}")

except Exception as exc:
    logger.critical("robô falhou", erro=str(exc), tipo=type(exc).__name__)
    raise

finally:
    restantes = logger.close()
    if restantes:
        print(f"{restantes} logs não confirmados — sobem na próxima execução")
```

---

## Referência da `RPALoggerConfig`

| Campo | Tipo | Padrão | Para que serve |
|---|---|---|---|
| `api_key` | `str` | — | **Obrigatório.** Chave do robô. Determina o `robot_id` no servidor. |
| `api_base_url` | `str` | — | **Obrigatório.** Base da API, ex.: `https://logs.suaempresa.com`. |
| `buffer_path` | `str` | `"./rpa_buffer.db"` | Arquivo do buffer local. Use caminho absoluto. |
| `log_timezone` | `str` | `"America/Sao_Paulo"` | Fuso em que o `created_at` do buffer é gravado. Ver [Fuso horário](#fuso-horário). |
| `log_utc_offset_hours` | `float` | `-3.0` | Offset usado quando a máquina não tem a base IANA (Windows sem `tzdata`). |
| `license_grace_hours` | `float` | `24.0` | Quanto tempo o robô pode rodar **sem** conseguir validar a chave. Só vale enquanto o servidor nunca respondeu — depois quem manda é o prazo do servidor. |
| `license_check_interval_minutes` | `float` | `30.0` | Intervalo de revalidação. Piso interno de 5s: valores menores são elevados. |
| `batch_size` | `int` | `500` | Logs por requisição. **Não passe de 1000** — a API rejeita lotes maiores. |
| `flush_interval_seconds` | `float` | `1.0` | Espera do worker quando o buffer está vazio. |
| `http_timeout_seconds` | `float` | `15.0` | Timeout das chamadas à API. |
| `verify_tls` | `bool` | `True` | Só desligue em teste local com certificado próprio. |
| `max_buffered_logs` | `int` | `100_000` | Teto do buffer. No estouro, descarta primeiro os **info/debug** mais antigos, preservando crítico e erro. |
| `on_warning` | `Callable[[str], None] \| None` | `None` | Canal de aviso da lib para a sua aplicação. `None` = silêncio total. |

### Propriedades úteis do cliente

| Propriedade | O que devolve |
|---|---|
| `logger.robot_id` | Quem o **servidor** diz que você é (conhecido após a primeira validação). |
| `logger.execution_id` | UUID desta execução. Agrupa os logs no banco. |
| `logger.is_active` | `False` quando o kill switch foi acionado. |
| `logger.pending_logs` | Logs ainda não confirmados, incluindo backlog de execuções anteriores. |

---

## Fuso horário

Desde a **v1.1.1** o `created_at` do `rpa_buffer.db` é gravado no **fuso local (-03:00)**, em
texto ISO 8601 com o offset dentro do valor:

```
2026-08-03T08:58:12.345678-03:00
```

Antes era epoch UNIX. O instante estava correto, mas qualquer ferramenta que abrisse o arquivo
o lia como UTC — e um log gravado às **08:58** aparecia como **11:58**.

Cada log carrega **dois** timestamps do mesmo instante, e é de propósito:

| Onde | Formato | Para quê |
|---|---|---|
| `created_at` (coluna do buffer) | texto local `-03:00` | leitura humana, ordenação da fila de envio |
| `timestamp` (dentro do `payload`) | epoch UNIX | é o que trafega para a API e vira `TIMESTAMPTZ` no banco |

O `timestamp` **não** é deslocado. Ele é o instante absoluto que a API valida; somá-lo a um
offset faria o `logged_at` divergir 3h do `received_at` do servidor e transformaria qualquer
medida de latência de ingestão em lixo. Quem converte para -03:00 na exibição é a API.

**Buffer de versão anterior é migrado automaticamente** na primeira abertura: a coluna é
convertida de epoch para texto local sem perder nenhum log pendente, nem o `attempts`, nem o
estado do kill switch. Nada a fazer manualmente.

Para trocar o fuso (ou rodar em UTC), use a config:

```python
RPALoggerConfig(..., log_timezone="America/Manaus", log_utc_offset_hours=-4.0)
```

> No Windows o `zoneinfo` só encontra a base IANA se o pacote `tzdata` estiver instalado.
> Sem ela a lib usa `log_utc_offset_hours` (-3 por padrão) — que é exatamente o mesmo
> resultado, já que o Brasil não tem horário de verão desde 2019. Instalar `tzdata` só
> importa se o horário de verão voltar.

---

## Níveis

| Constante | Valor | Uso |
|---|---|---|
| `LEVEL_CRITICAL` | 1 | robô parou / intervenção humana necessária |
| `LEVEL_ERROR` | 2 | operação falhou |
| `LEVEL_WARNING` | 3 | situação inesperada, robô seguiu |
| `LEVEL_INFO` | 4 | marco de execução (default) |
| `LEVEL_DEBUG` | 5 | detalhe de diagnóstico |

Métodos de conveniência: `critical()`, `error()`, `warning()`, `info()`, `debug()` — todos
aceitam `message` e `**context`. Ou `log(message, level=..., **context)`.

O `context` é JSON livre: `logger.error("falha no item", item_id=42, tentativa=3)`.

---

## Kill switch (R2)

A regra é uma só:

```
para  ⇔  chave inativa  OU  (agora − última validação bem-sucedida) > prazo
```

| Resposta da API | O robô |
|---|---|
| chave ativa | segue, e o prazo é renovado |
| **chave inativa** (explícito) | **para** no próximo checkpoint |
| chave desconhecida (404/401) | **não para** — pode ser rotação de chave; entra no prazo |
| API fora / erro de rede / 5xx | **não para** — enquanto o prazo não vencer |
| prazo vencido sem validação | **para** |

A distinção entre "a API disse que está inativa" e "a API não disse nada" é deliberada e não
pode ser relaxada: derrubar um cliente pagante por instabilidade de rede é o pior erro
possível aqui. Por outro lado, o prazo impede que "API fora" autorize rodar para sempre.

A validação acontece de duas formas: no intervalo configurado, e **de carona** na resposta de
cada envio de logs — quem está logando já está falando com a API, então a resposta renova o
prazo sem custo extra.

O prazo (`lease_seconds`) é definido pelo **servidor**; `license_grace_hours` é só o fallback
usado enquanto o servidor nunca respondeu. A última validação é persistida no buffer, então
reiniciar o robô **não** zera o prazo.

---

## Garantia de entrega (R1)

O que sustenta a garantia, elo por elo:

1. `log()` grava no SQLite **antes** de qualquer rede — se o processo morrer agora, o log está no disco;
2. o worker envia em lote e só remove do buffer os `log_id` que a API **confirmou**;
3. a API só confirma o que o broker confirmou ter persistido;
4. cada log tem um `log_id` único, e o servidor **deduplica** por ele.

Consequência prática: **reenviar é sempre seguro**. Se um POST der timeout depois de o
servidor já ter gravado, o reenvio não duplica nada.

O buffer usa WAL com `synchronous=NORMAL`: sobrevive a crash do processo (exceção, kill, robô
fechado à força). Perda só ocorreria em queda de energia ou do sistema operacional — troca
deliberada por ordens de magnitude de throughput, porque uma etapa de RPA gera dezenas de
milhares de logs.

---

## Erros comuns

| Não faça | Por quê |
|---|---|
| chave de API no código-fonte | vaza no git; a chave identifica o robô e autoriza a ingestão |
| `buffer_path` relativo | depende do diretório de trabalho; o backlog da execução anterior fica órfão |
| dois robôs no mesmo `buffer_path` | o worker drena tudo com a *sua* chave e os logs trocam de dono |
| `batch_size` acima de 1000 | a API rejeita o lote e os logs travam no buffer |
| esquecer o `logger.close()` | o buffer não é drenado no encerramento; os logs só sobem na execução seguinte |
| tratar `close() > 0` como perda | não é perda — é backlog, e ele sobe depois |
| esperar que a lib pare o robô sozinha | a parada é **cooperativa**: sem `raise_if_inactive()`, o kill switch não interrompe nada |
| `verify_tls=False` em produção | desliga a verificação do certificado |

---

## Mapa interno

| Módulo | Responsabilidade |
|---|---|
| `client.py` | `RPALoggerClient`: API pública + worker (thread daemon) que drena o buffer, revalida a licença e aplica backoff |
| `buffer.py` | `Buffer`: store-and-forward em SQLite; a única saída de um log é `mark_sent`, após confirmação |
| `transport.py` | `HttpTransport`: único canal de saída (HTTPS). Traduz status HTTP em decisão (confirmar, reter, parar) |
| `license.py` | `LicenseGuard`: política do kill switch. Puro — não faz I/O |
| `config.py` | `RPALoggerConfig`: configuração explícita, recebida da aplicação |

A lib é segura para chamar `log()` de várias threads (o buffer é protegido por lock).

---

## Exemplos

Em `examples/`, rodáveis com `pip install -e ".[dev]"`:

- **`exemplo_rpa.py`** — RPA completo: config, níveis, checkpoints, tratamento de
  `RPALicenseError` e encerramento. `python examples/exemplo_rpa.py <API_KEY> [n_etapas]`
- **`smoke_ingest.py`** — prova de R1 isolada: mostra que nada sai do buffer sem confirmação,
  inclusive com a API inalcançável. `python examples/smoke_ingest.py <API_KEY>`

---

## Migração da v1.0.0

A v2.0.0 é **breaking**. Na v1.0.0 a lib publicava direto no RabbitMQ e recebia a URL do
broker (com usuário e senha) da API. Agora ela só fala HTTPS com a API.

| v1.0.0 | v2.0.0 |
|---|---|
| `from rpa_logger.client import RPALoggerClient` | `from trajetoria_logger import RPALoggerClient` |
| dependia de `pika` | só `requests` |
| recebia `rabbitmq_url` da API | não recebe credencial de infraestrutura nenhuma |
| `robot_id` ia no payload (forjável) | derivado da chave, no servidor |
| fail-open infinito | fail-open **com prazo** (`lease_seconds`) |
| `publisher.py` | `transport.py` |

Para migrar: troque os imports, remova `pika` das dependências e confira que
`api_base_url` aponta para HTTPS. O resto da API pública (`info()`, `error()`,
`raise_if_inactive()`, `close()`) não mudou.
