#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>

#define MSG_SIZE 512 // макс длина одного сообщения
#define USERNAME_SIZE 32 // макс длина имени клиента

typedef struct data {
    int socket; // сокет клиента (Это файловый дескриптор для общения с клиентом)
    char username[USERNAME_SIZE];
    char client_ip[16];
    int client_port;
    int status;
    struct data *next; // указатель на след клиента
} Client;

typedef struct sockaddr_in sockaddr_in; // Это структура для хранения интернет-адреса (IP + порт)
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; // создание мьютекса (Это "замок", который позволяет только одному потоку одновременно работать с общими данными.)

Client *ClientList = NULL;
volatile sig_atomic_t flag = 1; // флаг работы сервера
int server_socket = -1; // сокет сервера

void handler(int sig);

void LogDisconnect(const char *username, const char *ip, int port, const char *reason) {// записывает информациб об отключении пользователя
   printf(" ОТКЛЮЧЕНИЕ: '%s' (%s:%d) - %s\n",  username, ip, port, reason);
}

void FreeList(Client *ClientList) { // освоюождение списка
    if(ClientList) {
        FreeList(ClientList->next);
        shutdown(ClientList->socket, SHUT_RDWR);
        close(ClientList->socket);
        free(ClientList);
    }
}

void handler(int sig) { // орабатывает сигнал кнтрл+ц и останавливает сервер
    flag = 0;
    printf("\nСЕРВЕР ЗАКРЫТ\n");
    if (server_socket != -1) {
        shutdown(server_socket, SHUT_RDWR);
        close(server_socket);
    }
    FreeList(ClientList);
}

void RemoveClient(int client_socket) { // удаляет клиента из списка
    pthread_mutex_lock(&mutex);
    Client *tmp = ClientList;
    Client *prev = NULL;

    if (!ClientList) { // если список пуст открываем мьютекс
        pthread_mutex_unlock(&mutex);
        return;
    }
    while(tmp && (tmp->socket != client_socket)) { // пока не конец и не нашли
        prev = tmp;
        tmp = tmp->next;
    }
    if (prev) {
        prev->next = tmp->next;
    }
    else {
        ClientList = tmp->next;
    }
    free(tmp);
    pthread_mutex_unlock(&mutex);
}

void AddClient(int client_socket, char *username, char *client_ip, int client_port, Client **ClientList) { //добавляет клиента в начало списка
    pthread_mutex_lock(&mutex);
    Client *tmp = malloc(sizeof(Client));
    tmp->next = *ClientList;
    strcpy(tmp->username, username);
    strcpy(tmp->client_ip, client_ip);
    tmp->client_port = client_port;
    tmp->socket = client_socket;
    tmp->status = 1;
    *ClientList = tmp;
    pthread_mutex_unlock(&mutex);
}

void SendList(int client_socket) { // отправляет список клиентов клиенту
    pthread_mutex_lock(&mutex);
    char user_list[MSG_SIZE] = "СПИСОК ПОЛЬЗОВАТЕЛЕЙ:\n";
    Client *tmp;
    for (tmp = ClientList; tmp; tmp = tmp->next) {
        if (tmp->status) {
            strcat(user_list, "- ");
            strcat(user_list, tmp->username);
            strcat(user_list, " (");
            strcat(user_list, tmp->client_ip);
            strcat(user_list, ":");
            char port_str[10];
            sprintf(port_str, "%d", tmp->client_port);
            strcat(user_list, port_str);
            strcat(user_list, ")");
            strcat(user_list, "\n");
        }
    }
    send(client_socket, user_list, strlen(user_list), 0);
    pthread_mutex_unlock(&mutex);
}

void SendPrivate(char *receiver, char* message, char *sender, int client_socket) { // имя получателя, текст, имя отправителя, сокет отправителя

    pthread_mutex_lock(&mutex);
    char msg[MSG_SIZE + USERNAME_SIZE];
    snprintf(msg, sizeof(msg), "%s: %s", sender, message);

    Client *tmp = ClientList;
    for(; tmp; tmp = tmp->next) {
        if (strcmp(tmp->username, receiver) == 0) {
            send(tmp->socket, msg, strlen(msg), 0);
            break;
        }
    }
    pthread_mutex_unlock(&mutex);
}

void SendMessage(char *message, char *sender, int client_socket) { // отправляет сообщение всем
    pthread_mutex_lock(&mutex);
    char msg[MSG_SIZE + USERNAME_SIZE];
    snprintf(msg, sizeof(msg), "%s: %s", sender, message);

    Client *tmp = ClientList;
    for(; tmp; tmp = tmp->next) {
        if (tmp->socket != client_socket) {
            send(tmp->socket, msg, strlen(msg), 0);
        }
    }
    pthread_mutex_unlock(&mutex);
}

void InitClient(int client_socket, char *username, char *client_ip, int client_port) {
    char msg[MSG_SIZE] = {0};
    char usern[USERNAME_SIZE] = {0};
    
    strcat(msg, "ВВЕДИТЕ: /user_list ЧТОБЫ УВИДЕТЬ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ\n");
    strcat(msg, "ЕСЛИ ХОТИТЕ ОТПРАВИТЬ ЛИЧНОЕ СООБЩЕНИЕ, ВВЕДИТЕ В ФОРМАТЕ:\n /private имя_пользователя сообщение\n");
    strcat(msg, "ЕСЛИ ХОТИТЕ ВЫЙТИ ИЗ СЕРВЕРА, НАПИШИТЕ /exit\n");
    strcat(msg, "ВВЕДИТЕ ИМЯ ПОЛЬЗОВАТЕЛЯ: \n");
    
    send(client_socket, msg, strlen(msg), 0); 
    
    int len = recv(client_socket, usern, sizeof(usern), 0);
    if (len <= 0) {
        return;
    }
    usern[len] = '\0';
    if (usern[len - 1] == '\n') {
        usern[len - 1] = '\0';
    }

    AddClient(client_socket, usern, client_ip, client_port, &ClientList);
    strcpy(username, usern);
    
    printf(" ПОДКЛЮЧЕНИЕ: '%s' (%s:%d)\n",  usern, client_ip, client_port);
}

void *ServeClient(void *arg) {
    int client_socket = (*(int *)arg); // присваивает дискриптор сокета через разыменование в интеджер
    free(arg);
    char sender[USERNAME_SIZE];
    char client_ip[16];
    int client_port;
    
    sockaddr_in client_addr; // объявление структуры для хранения информации о сетевом адресе клиента.
    socklen_t addr_len = sizeof(client_addr); // получение размера структуры адреса клиента для передачи в системные функции
    getpeername(client_socket, (struct sockaddr*)&client_addr, &addr_len); //запрашивает у операционной системы информацию о том, кто подключился к сокету Заполняет структуру client_addr IP и портом клиента
    strcpy(client_ip, inet_ntoa(client_addr.sin_addr));//Это преобразование IP-адреса из бинарного формата в текстовую строку
    client_port = ntohs(client_addr.sin_port); //преобразование номера порта из сетевого порядка байт в порядок байт процессора
    
    InitClient(client_socket, sender, client_ip, client_port);
    
    char msg[MSG_SIZE];
    int len;
    while (1) {
        memset(msg, 0, sizeof(msg));//очистка буфера сообщения - заполнение всего массива нулями
        len = recv(client_socket, msg, sizeof(msg), 0);
        if (len <= 0) {
            if (len == 0) {
                LogDisconnect(sender, client_ip, client_port, "Соединение закрыто клиентом");
            } else {
                LogDisconnect(sender, client_ip, client_port, "Ошибка соединения");
            }
            break;
        }
        msg[len] = '\0';
        if (strncmp(msg, "/user_list", 10) == 0) { // сравнивает первые 10 символов
            SendList(client_socket);
        }
        else if (strncmp(msg, "/private", 8) == 0) {
            char *name_finish = strchr(msg + 9, ' ');
            if (!name_finish) {
                SendMessage("НЕВЕРНЫЙ ВВОД", "Сервер", client_socket);
            }
            *name_finish = '\0';

            char *receiver;
            char *tmp;
            receiver = msg + 9;
            tmp = name_finish + 1;
            SendPrivate(receiver, tmp, sender, client_socket);
        }
        else if (strncmp(msg, "/exit", 5) == 0) {
            LogDisconnect(sender, client_ip, client_port, "Команда /exit");
            break;
        }
        else {
            SendMessage(msg, sender, client_socket);
        }
    }
    RemoveClient(client_socket);
    shutdown(client_socket, SHUT_RDWR);
    close(client_socket);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("Использование: %s IP ПОРТ\n", argv[0]);
        return 1;
    }
    
    signal(SIGINT, handler); // если сигинт(значит кнтрл+ц то переходим в хендлер)
    const char *IP = argv[1];
    int Port = atoi(argv[2]);

    int socket_fd, party_fd; // файловый дексрптр серверного сокета, фд клиентского сокета
    sockaddr_in serv_addr, party_addr;

    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    server_socket = socket_fd;

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET; //Устанавливает поле sin_family структуры sockaddr_in в значение AF_INET
    inet_pton(AF_INET, IP, &serv_addr.sin_addr); // Преобразует текстовый IP-адрес в бинарный формат, понятный операционной системе
    serv_addr.sin_port = htons(Port); // Преобразует номер порта из формата процессора в сетевой порядок байт(без преобразования печатал бы)

    bind(socket_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)); // Привязывает сокет к конкретному сетевому адресу (IP + порт)
    listen(socket_fd, SOMAXCONN);//Переводит сокет в слушающий режим

    printf("Сервер запущен на %s:%d\n", IP, Port);  

    while(flag) {
        memset(&party_addr, 0, sizeof(party_addr));
        socklen_t party_len = sizeof(party_addr);
        
        party_fd = accept(socket_fd, (struct sockaddr *)&party_addr, &party_len); 
        if (party_fd < 0) {
            if (flag) perror("accept error");
            continue;
        }

        printf("НОВЫЙ КЛИЕНТ: %s --- %d\n", inet_ntoa(party_addr.sin_addr), ntohs(party_addr.sin_port));

        pthread_t TID; // Этот блок кода создает отдельный поток для каждого подключившегося клиента
        int *thread_arg = malloc(sizeof(int));
        *thread_arg = party_fd;
        pthread_create(&TID, NULL, &ServeClient, (void *)thread_arg);
        pthread_detach(TID);
    }
    
    shutdown(socket_fd, SHUT_RDWR);
    close(socket_fd);
    return 0;
}