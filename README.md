# Luna PG‑ADMIN – Back‑end

Este repositório contém o servidor Express responsável por integrar a
plataforma de administração da Luna com a UAZAPI, o banco de dados
PostgreSQL e a API da OpenAI. A estrutura foi refatorada para
organizar melhor as responsabilidades em módulos, facilitando a
manutenção e evolução do projeto.

## Principais características

- **Exportação de conversas** – Agora a lógica de coleta de
  mensagens, transcrição e chamada à OpenAI está encapsulada em
  `src/services/exportAnalysis.js`. A rota
  `POST /api/instances/:id/export-analysis` delega a geração do
  relatório a este módulo e sempre retorna um PDF com as sugestões.
- **Relatório em PDF** – A rota
  `GET /api/instances/:id/export-analysis.pdf` recebe o slug do
  cliente via query string (`client=<slug>`) e responde com um PDF
  contendo apenas as sugestões geradas pela IA. O parâmetro opcional
  `force=1` pode ser usado para ignorar o gate de mensagens já
  analisadas.
- **Estrutura modular** – As funções de acesso ao banco, geração de
  PDF, manipulação de texto, cálculo de horários e comunicação com a
  UAZAPI foram extraídas para submódulos em `src/`.

## Uso

1. Instale as dependências com `npm install`.
2. Defina as variáveis de ambiente no arquivo `.env` (consulte
   `src/config.js` para a lista completa). Em particular, defina
   `OPENAI_API_KEY` e `UAZAPI_ADMIN_TOKEN` para habilitar a geração de
   relatórios.
3. Inicie o servidor com `node server.js` ou `npm start`.
4. Para gerar um relatório de análise de conversas, faça:

   ```
   POST /api/instances/<id>/export-analysis?client=cliente_slug&force=1
   ```

   O servidor retornará um arquivo PDF com as sugestões.

5. Para baixar diretamente o PDF, utilize:

   ```
   GET /api/instances/<id>/export-analysis.pdf?client=cliente_slug
   ```

   Inclua `force=1` se desejar ignorar o controle de mensagens já
   analisadas.