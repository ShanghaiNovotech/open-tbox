#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <termios.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <glib.h>

static gchar *g_if_serial_port = "/dev/ttymxc1";
static gint g_if_serial_baudrate = 9600;
static GMainLoop *g_if_main_loop = NULL;
static int g_if_serial_fd = -1;
static GIOChannel *g_if_serial_channel = NULL;
static GByteArray *g_if_read_buffer = NULL;

static GOptionEntry g_if_main_cmd_entries[] =
{
    { "port", 'P', 0, G_OPTION_ARG_STRING, &g_if_serial_port,
        "Set modem serial port", NULL },
    { "baudrate", 'B', 0, G_OPTION_ARG_INT, &g_if_serial_baudrate,
        "Set modem serial baud rate", NULL },
    { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

static gboolean if_serial_command_send_timeout(gpointer user_data)
{
    write(g_if_serial_fd, "AT+ICCID\r\n", 10);
    return TRUE;
}

static gboolean if_serial_wait_timeout(gpointer user_data)
{
    g_main_loop_quit(g_if_main_loop);
    return FALSE;
}

static gboolean if_serial_io_watch_cb(GIOChannel *source,
    GIOCondition condition, gpointer user_data)
{
    ssize_t rsize;
    guint8 buffer[4096];
    gchar iccid[32] = {0};
    gchar *tmp;
    gboolean flag = FALSE;
    
    while((rsize=read(g_if_serial_fd, buffer, 4096))>0)
    {
        g_byte_array_append(g_if_read_buffer, buffer, rsize);
    }
    
    tmp = g_strstr_len((const gchar *)g_if_read_buffer->data,
        g_if_read_buffer->len, "ICCID:");
    if(tmp!=NULL && strlen(tmp)>6 && strchr(tmp, '\n')!=NULL)
    {
        strncpy(iccid, tmp+6, 31);
        
        g_strdelimit(iccid, "\r\n", '\0');
        
        tmp = g_strstrip(iccid);
        
        if(strlen(tmp)>=19)
        {
            printf("%s\n", tmp);
            flag = TRUE;
        }
        else
        {
            g_byte_array_unref(g_if_read_buffer);
            g_if_read_buffer = g_byte_array_new();
        }
    }
    
    if(flag)
    {
        g_main_loop_quit(g_if_main_loop);
        return FALSE;
    }
    
    if(g_if_read_buffer->len > 1024 * 1024)
    {
        g_byte_array_unref(g_if_read_buffer);
        g_if_read_buffer = g_byte_array_new();
    }
    
    return TRUE;
}

int main(int argc, char *argv[])
{
    GError *error = NULL;
    GOptionContext *context;
    struct termios options;
    int fd;
    int rate;
    
    context = g_option_context_new("- ICCID Fetch");
    g_option_context_add_main_entries(context, g_if_main_cmd_entries,
        "ICCID Fetch");
    if(!g_option_context_parse(context, &argc, &argv, &error))
    {
        g_warning("Option parsing failed: %s\n", error->message);
        g_clear_error(&error);
    }
    
    fd = open(g_if_serial_port, O_RDWR | O_NOCTTY | O_NDELAY);
    if(fd<0)
    {
        g_error("Cannot open serial port %s: %s", g_if_serial_port,
            strerror(errno));
        return 1;
    }
    
    switch(g_if_serial_baudrate)
    {
        case 300:
        {
            rate = B300;
            break;
        }
        case 600:
        {
            rate = B600;
            break;
        }
        case 1200:
        {
            rate = B1200;
            break;
        }
        case 2400:
        {
            rate = B2400;
            break;
        }
        case 4800:
        {
            rate = B4800;
            break;
        }
        case 9600:
        {
            rate = B9600;
            break;
        }
        case 19200:
        {
            rate = B19200;
            break;
        }
        case 38400:
        {
            rate = B38400;
            break;
        }
        case 57600:
        {
            rate = B57600;
            break;
        }
        case 115200:
        {
            rate = B115200;
            break;
        }
        default:
        {
            rate = 9600;
            break;
        }
    }
    
    tcgetattr(fd, &options);
    cfmakeraw(&options);
    cfsetispeed(&options, rate);
    cfsetospeed(&options, rate);
    options.c_cflag &= ~CSIZE;
    options.c_cflag &= ~PARENB;
    options.c_cflag &= ~PARODD;
    options.c_cflag &= ~CSTOPB;
    options.c_cflag &= ~CRTSCTS;
    options.c_cflag |= CS8;
    options.c_cflag |= (CLOCAL | CREAD);
    options.c_iflag &= ~(IGNBRK | BRKINT | ICRNL |
        INLCR | PARMRK | INPCK | ISTRIP | IXON | IXOFF | IXANY);
    options.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG | IEXTEN);
    options.c_oflag &= ~OPOST;
    options.c_cc[VMIN] = 1;
    options.c_cc[VTIME] = 0;
    tcflush(fd, TCIOFLUSH);
    tcsetattr(fd, TCSANOW, &options);
    
    g_if_serial_fd = fd;
    g_if_serial_channel = g_io_channel_unix_new(fd);
    if(g_if_serial_channel==NULL)
    {
        g_error("Cannot create serial IO channel!");
        return 2;
    }
    
    g_if_read_buffer = g_byte_array_new();
    
    g_if_main_loop = g_main_loop_new(NULL, FALSE);
    
    g_io_add_watch(g_if_serial_channel, G_IO_IN, if_serial_io_watch_cb, NULL);
    
    g_timeout_add_seconds(5, if_serial_command_send_timeout, NULL);
    g_timeout_add_seconds(60, if_serial_wait_timeout, NULL);
    
    if_serial_command_send_timeout(NULL);
    
    g_main_loop_run(g_if_main_loop);
    g_main_loop_unref(g_if_main_loop);
    g_if_main_loop = NULL;
    
    g_byte_array_unref(g_if_read_buffer);
    g_if_read_buffer = NULL;
    
    g_io_channel_unref(g_if_serial_channel);
    g_if_serial_channel = NULL;
    
    close(g_if_serial_fd);
    g_if_serial_fd = -1;
    
    return 0;
}
