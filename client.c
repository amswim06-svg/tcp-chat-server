#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include <arpa/inet.h>

typedef struct sockaddr_in sockaddr_in;
enum { MSG_SIZE = 512 };
volatile sig_atomic_t running = 1; // флаг работы программы

void handler(int sig) { running = 0; }

int main(int argc, char **argv) {
    signal(SIGINT, handler);
    if (argc != 3) {
        printf("Использование: %s АЙПИ ПОРТ\n", argv[0]);
        printf("Пример: %s 127.0.0.1 5000\n", argv[0]);
        return 1;
    }
    
    const char *IP = argv[1];
    int Port = atoi(argv[2]);
    
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(Port);
    inet_pton(AF_INET, IP, &server_addr.sin_addr); // Преобразует текстовый IP-адрес в бинарный формат
    
    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) { //Устанавливает TCP-соединение с указанным сервером
        perror("Не удалось подключиться");
        return 1;
    }
    printf("Подключено к серверу %s:%d\n", IP, Port);

    struct pollfd fds[2];
    char msg[MSG_SIZE];
    fds[0].fd = STDIN_FILENO;
    fds[0].events = POLLIN; 
    fds[1].fd = sock_fd; // Дескриптор подключенного сокета
    fds[1].events = POLLIN; // Интересует "пришли данные с сервера"
    
    while(running) {
        poll(fds, 2, -1); // позволяет клиенту одновременно ждать событий от нескольких источников.
        
        if(fds[1].revents & POLLIN) { 
            memset(msg, 0, sizeof(msg));
            int len = recv(sock_fd, msg, sizeof(msg), 0);
            if (len <= 0) {
                printf("Сервер отключился\n");
                break;
            }
            printf("%s", msg);
            fflush(stdout);
        }
        
        if(fds[0].revents & POLLIN) {
            if (fgets(msg, sizeof(msg), stdin) == NULL) break;
            send(sock_fd, msg, strlen(msg), 0);
            if (strncmp(msg, "/exit", 5) == 0) {
                printf("Выход...\n");
                break;
            }
        }
    }
    shutdown(sock_fd, SHUT_RDWR);
    close(sock_fd);
    printf("Клиент остановлен\n");
    return 0;
}