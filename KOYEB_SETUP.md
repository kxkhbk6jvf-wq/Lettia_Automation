# Setup da Aplicação Lettia Automation no Koyeb

Este guia explica como fazer o deploy da aplicação Lettia Automation na plataforma Koyeb.

## Pré-requisitos

1. Conta no Koyeb (https://www.koyeb.com)
2. Repositório Git (GitHub, GitLab, ou Bitbucket) com o código
3. Todas as variáveis de ambiente configuradas

## Opções de Deploy

### Opção 1: Deploy via Git (Recomendado)

1. **Conecte seu repositório ao Koyeb:**
   - Acesse o dashboard do Koyeb
   - Clique em "Create App" ou "New Service"
   - Selecione "GitHub", "GitLab" ou "Bitbucket"
   - Autorize o acesso ao seu repositório
   - Selecione o repositório `Lettia_Automation-2`

2. **Configure o Build:**
   - Build Type: `Dockerfile`
   - Dockerfile Path: `Dockerfile` (ou deixe em branco se estiver na raiz)
   - Build Command: (deixe em branco, o Dockerfile já faz o build)

3. **Configure o Run:**
   - Run Command: `python koyeb_entrypoint.py`
   - Ou deixe em branco para usar o CMD do Dockerfile

### Opção 2: Deploy via Docker Image

1. **Build e push da imagem Docker:**
   ```bash
   docker build -t lettia-automation .
   docker tag lettia-automation:latest <seu-registry>/lettia-automation:latest
   docker push <seu-registry>/lettia-automation:latest
   ```

2. **No Koyeb:**
   - Crie um novo serviço
   - Selecione "Docker" como origem
   - Informe a URL da imagem Docker

## Configuração de Variáveis de Ambiente

No dashboard do Koyeb, configure as seguintes variáveis de ambiente na seção "Environment Variables":

### Variáveis Obrigatórias

```env
# Lodgify Configuration
LODGIFY_API_KEY=your_api_key_here
LODGIFY_PROPERTY_ID=your_property_id_here

# Google Services Configuration
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"...","private_key_id":"...","private_key":"...","client_email":"...","client_id":"...","auth_uri":"...","token_uri":"...","auth_provider_x509_cert_url":"...","client_x509_cert_url":"..."}
GOOGLE_SHEET_RESERVATIONS_ID=your_reservations_sheet_id
GOOGLE_SHEET_SEF_ID=your_sef_sheet_id
GOOGLE_SHEET_SEF_TEMPLATE_ID=your_sef_template_sheet_id  # Opcional

# Dropbox Configuration
DROPBOX_ACCESS_TOKEN=your_dropbox_access_token
DROPBOX_SEF_FOLDER=/SEF

# WhatsApp Configuration
WHATSAPP_TOKEN=your_whatsapp_token
WHATSAPP_PHONE_NUMBER_ID=your_phone_number_id
OWNER_PHONE=+351XXXXXXXXX

# Financial Configuration
VAT_RATE=0.06
AIRBNB_FEE_PERCENT=0.03
LODGIFY_FEE_PERCENT=0.02
STRIPE_FEE_TABLE={"fee_type":"percentage","rate":0.029,"fixed":0.30}
```

### Variáveis de Configuração do Koyeb

```env
# Modo de execução: 'health', 'task', ou 'scheduled'
KOYEB_MODE=task

# Nome da tarefa a executar (quando KOYEB_MODE=task)
KOYEB_TASK=process_sef

# Intervalo em segundos para modo scheduled (padrão: 3600 = 1 hora)
KOYEB_SCHEDULE_INTERVAL=3600
```

## Modos de Execução

### 1. Modo Task (Padrão)
Executa uma tarefa única e termina. Útil para jobs agendados via cron do Koyeb.

```env
KOYEB_MODE=task
KOYEB_TASK=process_sef
```

Tarefas disponíveis:
- `process_sef` - Processa formulários SEF e gera PDFs
- `sync_lodgify` - Sincroniza reservas do Lodgify
- `generate_invoices` - Gera faturas
- `full_cycle` - Executa todas as tarefas
- Ver todas as tarefas: execute `python -m core.scheduler --list`

### 2. Modo Scheduled
Executa uma tarefa repetidamente em intervalos regulares.

```env
KOYEB_MODE=scheduled
KOYEB_TASK=process_sef
KOYEB_SCHEDULE_INTERVAL=3600  # 1 hora em segundos
```

### 3. Modo Health Check
Usado para health checks do Koyeb.

```env
KOYEB_MODE=health
```

## Configurando Jobs Agendados no Koyeb

Para executar tarefas em horários específicos, você pode usar o recurso de Cron Jobs do Koyeb:

1. No dashboard do Koyeb, vá para "Cron Jobs"
2. Crie um novo cron job
3. Configure:
   - **Schedule**: Use formato cron (ex: `0 */6 * * *` para a cada 6 horas)
   - **Service**: Selecione seu serviço Lettia Automation
   - **Command**: Deixe em branco (usa o CMD do Dockerfile)
   - **Environment Variables**: Adicione `KOYEB_MODE=task` e `KOYEB_TASK=<nome_da_tarefa>`

### Exemplos de Schedules

- A cada hora: `0 * * * *`
- A cada 6 horas: `0 */6 * * *`
- Diariamente às 2h: `0 2 * * *`
- A cada 30 minutos: `*/30 * * * *`

## Verificando o Deploy

1. **Logs:**
   - Acesse a aba "Logs" no dashboard do Koyeb
   - Verifique se não há erros de inicialização

2. **Health Check:**
   - O Koyeb verifica automaticamente o health check
   - Você pode testar manualmente executando com `KOYEB_MODE=health`

3. **Testar uma Tarefa:**
   - Configure `KOYEB_MODE=task` e `KOYEB_TASK=process_sef`
   - Verifique os logs para confirmar a execução

## Troubleshooting

### Erro: "Module not found"
- Verifique se todas as dependências estão no `requirements.txt`
- O Dockerfile instala automaticamente, mas pode precisar de rebuild

### Erro: "Environment variable not set"
- Verifique se todas as variáveis obrigatórias estão configuradas no Koyeb
- Lembre-se que `GOOGLE_SERVICE_ACCOUNT_JSON` deve ser uma string JSON completa

### Erro: "Task not found"
- Execute `python -m core.scheduler --list` localmente para ver tarefas disponíveis
- Verifique se o nome da tarefa em `KOYEB_TASK` está correto

### Health Check falhando
- Verifique os logs para erros de inicialização
- Certifique-se de que o Python path está configurado corretamente

## Estrutura de Arquivos para Koyeb

```
Lettia_Automation-2/
├── Dockerfile              # Configuração Docker
├── koyeb.toml             # Configuração Koyeb (opcional)
├── koyeb_entrypoint.py    # Script de entrada para Koyeb
├── requirements.txt       # Dependências Python
└── ...                    # Resto do código
```

## Próximos Passos

1. Faça o deploy inicial
2. Configure as variáveis de ambiente
3. Teste com uma tarefa simples (`process_sef`)
4. Configure cron jobs para automação
5. Monitore os logs regularmente

## Suporte

Para mais informações sobre o Koyeb:
- Documentação: https://www.koyeb.com/docs
- Status: https://status.koyeb.com

