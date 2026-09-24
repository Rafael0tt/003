using System.Collections.Concurrent; // Chamamos uma CAIXINHA de FERRAMENTAS que ajudam a GUARDAR DADOS de forma SEGURA quando várias coisas estão acontecendo ao mesmo tempo.
using System.Net; // Pegamos as FERRAMENTAS que SABEM MEXER com ENDEREÇOS e CONEXÔES DE REDE E INTERNET.
using System.Net.WebSockets; // Ferramentas feitas ESPECIFICAMENTE para CRIAR CHATS em TEMPO REAL.
using System.Text; // O tradutor que CONVERTE TEXTOS em pedacinhos de BYTES que COMPUTADOR consegue enviar pela INTERNET.


ConcurrentDictionary<string, WebSocket> clientes = new ConcurrentDictionary<string, WebSocket>(); // Críamos um ARMÁRIO SUPER SEGURO que vai GUARDAR TODOS OS ÚSUARIOS conectados no CHAT, usando um código único para cada um.

HttpListener servidor = new HttpListener(); // Criamos o NOSSO SERVIDOR de internet, que vai ficar pronto para RECEBER os visitantes.
servidor.Prefixes.Add("http://localhost:8080/"); // Dizemos o ENDEREÇO EXATO e a PORTA onde o nosso servidor vai MORAR no computador.
servidor.Start(); // Apertamos UM BOTÂO pra LIGAR o servidor duma vez!!!

Console.WriteLine("Servidor iniciado!"); // Escreva na tela preta dizendo que o servidor já está RODANDO.
Console.WriteLine("Abra http://localhost:8080 no navegador."); // Escreve na tela o LINK você terá que DIGITAR no navegador para ACESSAR o servidor.

while (true) // Criamos um CICLO(LOOP) que roda PARA SEMPRE, sem DESLIGAR NUNCA, para continuar atendendo novos visitantes.
{ // Abertura do blovo "WHILE/INÍCIO".
    HttpListenerContext context = await servidor.GetContextAsync(); // O programa PAUSA AQUI. E espera pacientemente ALGUÉM BATER NA PORTA DA INTERNET.

    if (context.Request.IsWebSocketRequest) // PERGUNTAMOS: "Será que esta pessoa quer entrar no chat por WebSocket?".
    { // Abertura do bloco "IF".
        HttpListenerWebSocketContext websocketContext = await context.AcceptWebSocketAsync(null); // Se a RESPOSTA for SIM, ABRIMOS A PORTA E ACEITAMOS A CONEXÃO DE CHAT.

        WebSocket cliente = websocketContext.WebSocket; // GUARDAMOS essa CONEXÃO recém-chegada DENTRO de uma CAIXINHA chamada "cliente".
        
        string clientId = Guid.NewGuid().ToString(); // Geramos um CRACHÁ/NÙMERO DE INDENTIFICAÇÃO/ID com um código único e aleatório para identificar esse cliente.

        clientes.TryAdd(clientId, cliente); // Colocamos o CRACHÁ/NÙMERO DE INDENTIFICAÇÃO/ID e o CLIENTE DENTRO do ARMÁRIO SEGURO de pessoas conectadas que CRIAMOS la atrás.

        Console.WriteLine("Novo cliente conectado!"); // Escreve na tela que mais alguém se conectou, e escrevemos "Novo cliente conectado".

        _ = Task.Run(async () => // Mandamos essa conversa rodar em SEGUNDO PLANO (em outra linha de raciocínio), para não TRAVAR O SERVIDOR.
    { // Fechamento do bloco "IF".
            byte[] buffer = new byte[1024]; // Pegamos um BALDE VAZIO(array de bytes) para RECEBER as mensagens que vão chegar.

            try // Agora nós VIGIA o código. Se a INTERNET  CAIR ou der ERRO, evitamos que o programa QUEBRE FEIO.
            { // Abertura do bloco "TRY".
                while (cliente.State == WebSocketState.Open) // (+ UM WHILE/LOOP) Enquanto a pessoa estiver com a PÁGINA ABERTA e o CHAT CONECTADO.
                { // Abertuda do bloco "WHILE".
                    WebSocketReceiveResult resultado = await cliente.ReceiveAsync // Ficamos eseperando o CLIENTE enviar alguma MENSAGEM.
                    ( // Abertura o () que é um COMANDO de RECEBIMENTO DE MENSAGEM.
                        new ArraySegment<byte>(buffer), // APONTAMOS para o BALDE onde os DADOS RECEBIDOS serão GUARDADOS.

                        CancellationToken.None // Indicamos que NÃO VAMOS CANCELAR essa escuta ANTES DO TEMPO.
                    ); // Fechamento o () que é um COMANDO de RECEBIMENTO DE MENSAGEM.

                    if (resultado.MessageType == WebSocketMessageType.Close) // (+ UM IF) Conferimos: "Será que o cliente fechou a página?".
                    { // ABertura do bloco "IF".
                        break; // Se ele FECHOU, usamos o break para QUEBRAR e SAIR IMEDIATAMENTE do CHAT/DA CONVERSA.
                    } // Fechadura do bloco "IF".

                    string mensagem = Encoding.UTF8.GetString(buffer, 0, resultado.Count); // TRADUZIMOS os bytes que vieram no BALDE DE VOLTA para um texto NORMAL QUE CONSEGUIMOS LER DE BOA.
                    Console.WriteLine("Mensagem: " + mensagem); // Mostramos a mensagem recebida na tela

                    foreach (var par in clientes) // Olhamos UM POR UM DE TODOS os amigos que estão guardados no nosso ARMÁRIO DE CLIENTES conectados.
                    { // Abertura do bloco "FOREACH".
                        WebSocket outroCliente = par.Value; // Em cada VOLTINHA, pegamos o COLEGA da VEZ.
                        if (outroCliente.State == WebSocketState.Open) // (+ UM IF) Conferimos: "Será que o COLEGA ESTÁ com o CHAT ABERTO e PRONTO para receber?"
                        { // Abertura do bloco "IF".
                            byte[] mensagemBytes = Encoding.UTF8.GetBytes(mensagem); // Transformamos o TEXTO da MENSAGEM em BYETS DE NOVO.
                            await outroCliente.SendAsync // Começamos a ENVIAR essa MENSAGEM para a TELA do COLEGA.
                            ( // Abertura do () que é um COMANDO de ENVIO.
                                new ArraySegment<byte>(mensagemBytes), // Apontamos para os BYTES da MENSAGEM.
                                WebSocketMessageType.Text, // DIZEMOS que o TIPO do que estamos mandando é TEXTO PURO.
                                true, // Confirmamos que a mensagem está indo de uma VEZ SÓ e INTEIRA.
                                CancellationToken.None // Dizemos que possívelmente não haverá cancelamento, ou seja, NENHUM CANCELAMENTO PROGRAMAOD.
                            ); // Fechamento dos () que é um COMANDO de ENVIO.
                        } // Fechamento do bloco "IF".
                    } // Fechamento do bloc "FOREACH".
                } // Fechamento do bloco "WHILE".
            } // Fechamento do bloco "TRY".
            catch // (A MUIÉ DO TRY/O PAR DO TRY) Se der QUALQUER B.O de CONEXÃO ou erro INESPERADO, o programa pula e cai pra cá.
            { // Abre a chave do "CATCH".
                Console.WriteLine("Cliente desconectado."); // Escreve "Cliente desconectado.", ou seja, o CLIENTE SAIU/CAIU do SERVIDOR.
            } // Fecha a chave de "CATCH".

            clientes.TryRemove(clientId, out _); // PEGAMOS O CRACHÁ E TIRAMOS AQUELE CLIENTE de dentro do nosso ARMÁRIO de conexões ativas.
            cliente.Dispose(); // APAGAMOS/LIMPAMOS TUDO o que sobrava daquele usuário na memória.
        });
    }
    else // Voltando lá no COMEÇO: se a pessoa NÃO QUIS ENTRAR no chat e pediu uma página normal de site.
    { // Abertura da chave do "ELSE".
        string caminho = Path.Combine(Directory.GetCurrentDirectory(), "index.html"); // Procura onde está salvo o arquivo INDEX.HTML no computador.

        if (File.Exists(caminho)) // (+ um IF) Conferimos: "O arquivo index.html realmente existe na pasta?".
        { // Abertura do bloco "IF".
            string html = await File.ReadAllTextAsync(caminho); // SE EXISTIR, ABRIMOS/LEMOS o arquivo e o que está escrito dentro dele.
            byte[] htmlBytes = Encoding.UTF8.GetBytes(html); // Transformamos TODO O TEXTO DA PÁGINA HTML em pedacinhos de bytes.

            context.Response.ContentType = "text/html"; // AVISAMOS ao NAVEGADORR que o que estamos mandando é uma PÁGINA DE SITE EM HTML.
            context.Response.ContentLength64 = htmlBytes.Length; // Informamos o TAMANHO exato do arquivo que VAMOS ENVIAR.

            await context.Response.OutputStream.WriteAsync(htmlBytes); // ENTREGAMOS O CONTEÚDO da página para o navegador abrir na tela.
            context.Response.Close(); // Fechamos a resposta HTTP, e TERMINAMOS e ENTREGAMOS da página.
        } // Fechamento do bloco "IF".
    } // Fechamento da chave do "ELSE".
} // Fechamento do bloco "WHILE/INÍCIO".