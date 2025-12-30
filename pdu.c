/*
 * PDU - PostgreSQL Data Unloader
 * Copyright (c) 2024-2025 ZhangChen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "read.h"
#include "tools.h"

int USR_CMD;

char former[50];
char latter[50];
char third[50];
char fourth[10240];

/**
 * parseCmd - Parse user input command string
 *
 * @command: Input command string buffer
 *
 * Parses the command string, extracting up to 4 tokens (former, latter,
 * third, fourth) and sets the global USR_CMD variable to the appropriate
 * command type constant.
 *
 * Returns: The parsed command type (USR_CMD value)
 */
int parseCmd(char command[MiddleAllocSize])
{
    memset(former,0,50);
    memset(latter,0,50);
    memset(third,0,50);
    memset(fourth,0,10240);

    if ( strcmp(command,"b") == 0){
        USR_CMD=CMD_BOOTSTRAP;
    }
    else if ( strcmp(command,"exit") == 0 || strcmp(command,"\\q") == 0){
        USR_CMD=CMD_EXIT;
    }
    else if ( strncmp(command, "use", 3) == 0 || 
              strncmp(command, "set", 3) == 0 || 
              strncmp(command, "\\", 1) == 0 || 
              strncmp(command, "unload", 6) == 0 || 
              strncmp(command, "scan", 4) == 0 ||
              strncmp(command, "add", 3) == 0 ||
              strncmp(command, "param", 5) == 0 ||
              strncmp(command, "show",4) == 0 ||
              strncmp(command, "reset",5) == 0 ||
              strncmp(command, "dropscan",8) == 0 ||
              strncmp(command, "info",4) == 0 ||
              strncmp(command, "restore", 7) == 0 || 
              strncmp(command,"ds",2) == 0 ||
              strcmp(command,"ckwal") == 0 ||
              strncmp(command,"meta",4) == 0 ||
              shortCMDMatched(command)
              )
    {
        int cmdcount=1;
        char *token=strtok(command, " ");
        while (token != NULL)
        {
            if( cmdcount==1 ){
                strcpy(former,token);
            }
            else if (cmdcount ==2){
                strcpy(latter,token);
            }
            else if (cmdcount ==3){
                strcpy(third,token);
            }
            else if (cmdcount ==4){
                strcpy(fourth,token);
            }
            token = strtok(NULL, " ");
            cmdcount++;
        }
        if( strcmp(former,"use")==0 ){
            USR_CMD=CMD_USE;
        }

        else if( strcmp(former,"set")==0 ){
            USR_CMD=CMD_SET;
        }

        else if( strcmp(former,"\\d")==0 ||strcmp(former,"\\d+")==0 ){
            USR_CMD=CMD_DESC;
        }

        else if( strncmp(former,"\\" , 1)==0 && strcmp(latter,"") == 0){
            USR_CMD=CMD_SHOW;
        }

        else if( strcmp(former,"unload")==0 || strcmp(former,"u")==0){
            USR_CMD=CMD_UNLOAD;
        }
        else if( strcmp(former,"t") == 0 && strcmp(latter,"") == 0){
            USR_CMD=CMD_SHOWTYPE;
        }
        else if( strcmp(former,"param")==0 || strcmp(former,"p")==0){
            USR_CMD=CMD_PARAM;
        }
        else if( strcmp(former,"reset")==0 ){
            USR_CMD=CMD_RESETPARAM;
        }
        else if( strcmp(former,"scan")==0 ){
            USR_CMD=CMD_SCAN;
        }
        else if( strcmp(former,"dropscan")==0 || strcmp(former,"ds")==0 ){
            USR_CMD=CMD_DROPSCAN;
        }
        else if( strcmp(former,"info")==0 || strcmp(former,"i")==0){
            USR_CMD=CMD_INFO;
        }
        else if( strcmp(former,"restore")==0 ){
            USR_CMD=CMD_RESTORE;
        }
        else if( strcmp(former,"ckwal")==0 ){
            USR_CMD=CMD_CHECKWAL;
        }
        else if( strcmp(former,"meta")==0 ){
            USR_CMD=CMD_META;
        }
        else if( strcmp(former,"add")==0 ){
            USR_CMD=CMD_ADD;
        }

        else if( strcmp(former,"show")==0 && strcmp(latter,"")==0 ){
            USR_CMD=CMD_SHOWPARAM;
        }
        else{
            USR_CMD=CMD_UNKNOWN;
        }
    }
    else{
        USR_CMD=CMD_UNKNOWN;
    }

    return USR_CMD; 
}

/**
 * execute_command_string - Execute a semicolon-separated command string
 *
 * @cmd_str: Command string containing one or more semicolon-separated commands
 *
 * Parses and executes each command in the string sequentially.
 * Commands are separated by semicolons. Whitespace around commands is trimmed.
 */
void execute_command_string(char *cmd_str) {
    char *saveptr = NULL;
    char *token = strtok_r(cmd_str, ";", &saveptr);

    while (token != NULL) {
        trim_whitespace(token);

        if (strlen(token) > 0) {
            parseCmd(token);
            execCmd(USR_CMD, former, latter, third, fourth);
        }

        token = strtok_r(NULL, ";", &saveptr);
    }
}


/**
 * main - Application entry point
 *
 * @argc: Argument count
 * @argv: Argument vector
 *
 * Main entry point for PDU (PostgreSQL Data Unloader). Supports both
 * interactive mode and command-line mode. In command-line mode, all
 * arguments are concatenated and executed as semicolon-separated commands.
 *
 * Returns: 0 on success, 1 on initialization failure
 */
int main(int argc, char *argv[]) {
    setup_crash_handlers();

    if(getInit() != 1){
        exit(1);
    }

    if (argc > 1) {
        char *cmd_str = (char *)malloc(10240 * sizeof(char));
        cmd_str[0] = '\0';

        for (int i = 1; i < argc; i++) {
            strcat(cmd_str, argv[i]);
            if (i < argc - 1) {
                strcat(cmd_str, " ");
            }
        }

        size_t len = strlen(cmd_str);
        if (len > 0 && cmd_str[len - 1] != ';') {
            strcat(cmd_str, ";");
        }

        execute_command_string(cmd_str);
        free(cmd_str);
        exit(0);
    }

    while (1) {
        char *cmd=NULL;
        cmd=getinput();
        parseCmd(cmd);
        execCmd(USR_CMD,former,latter,third,fourth);
    }
    return 0;
}

