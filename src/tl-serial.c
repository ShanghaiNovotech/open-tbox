#include <unistd.h>
#include <string.h>
#include <termios.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include "tl-serial.h"
#include "tl-main.h"

#define TL_SERIAL_WRITE_RETRY_MAXIUM 3
#define TL_SERIAL_READ_BUFFER_SIZE 512

#define TL_SERIAL_RETRY_TIMEOUT 5
#define TL_SERIAL_HEARTBEAT_TIMEOUT 5
#define TL_SERIAL_TIME_SYNC_TIMEOUT 120

typedef struct _TLSerialWriteData
{
    GByteArray *buffer;
    guint8 command;
    guint retry_count;
    gint64 timestamp;
}TLSerialWriteData;

typedef struct _TLSerialData
{
    gboolean initialized;
    int fd;
    GIOChannel *channel;
    
    guint8 read_buffer[TL_SERIAL_READ_BUFFER_SIZE];
    size_t read_size;
    size_t read_expect_size;
    
    GQueue *write_queue;
    TLSerialWriteData *write_data;
    size_t write_size;
    
    guint read_watch_id;
    guint write_watch_id;
    
    gint64 heartbeat_timestamp;
    gint64 time_sync_timestamp;
    
    gboolean time_sync_finished;
    
    gboolean low_voltage_shutdown;
    
    gboolean alarm_clock_enabled;
    gint64 alarm_clock_time;
    gint daily_alarm_clock_hour;
    gint daily_alarm_clock_min;
    guint8 gravity_threshold;
    
    guint check_timeout_id;
}TLSerialData;

static TLSerialData g_tl_serial_data = {0};

static void tl_serial_write_data_free(TLSerialWriteData *data)
{
    if(data==NULL)
    {
        return;
    }
    
    if(data->buffer!=NULL)
    {
        g_byte_array_unref(data->buffer);
    }
    
    g_free(data);
}

static TLSerialWriteData *tl_serial_write_data_new(GByteArray *buffer,
    guint8 command, gboolean need_ack)
{
    TLSerialWriteData *write_data;
    
    write_data = g_new0(TLSerialWriteData, 1);
    
    write_data->buffer = g_byte_array_ref(buffer);
    write_data->command = command;
    write_data->retry_count = need_ack ? TL_SERIAL_WRITE_RETRY_MAXIUM : 0;
    write_data->timestamp = g_get_monotonic_time();
    
    return write_data;
}

static gboolean tl_serial_write_io_watch_cb(GIOChannel *source,
    GIOCondition condition, gpointer user_data)
{
    TLSerialData *serial_data = (TLSerialData *)user_data;
    ssize_t rsize;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    if(condition | G_IO_OUT)
    {
        if(serial_data->write_data==NULL)
        {
            serial_data->write_data = g_queue_pop_head(
                serial_data->write_queue);
            serial_data->write_size = 0;
        }
        
        if(serial_data->write_data==NULL)
        {
            serial_data->write_watch_id = 0;
            return FALSE;
        }
        
        do
        {
            rsize = 0;
            while(serial_data->write_size <
                serial_data->write_data->buffer->len &&
                (rsize=write(serial_data->fd,
                serial_data->write_data->buffer->data +
                serial_data->write_size,
                serial_data->write_data->buffer->len -
                serial_data->write_size))>0)
            {
                serial_data->write_size += rsize;
            }
            
            if(serial_data->write_size >= serial_data->write_data->buffer->len)
            {
                if(serial_data->write_data->retry_count > 0)
                {
                    serial_data->write_data->timestamp =
                        g_get_monotonic_time();
                    serial_data->write_size = 0;
                    serial_data->write_watch_id = 0;
                    return FALSE;
                }
                else
                {
                    tl_serial_write_data_free(serial_data->write_data);
                    serial_data->write_data = NULL;
                }
            }
            
            if(rsize<=0)
            {
                break;
            }
        }
        while(serial_data->write_data!=NULL);
        
        if(serial_data->write_data==NULL &&
            g_queue_is_empty(serial_data->write_queue))
        {
            serial_data->write_watch_id = 0;
            return FALSE;
        }
    }
    
    return TRUE;
}

static void tl_serial_write_data_request(TLSerialData *serial_data,
    guint8 command, const guint8 *payload, guint8 length, gboolean need_ack)
{
    GByteArray *packet;
    TLSerialWriteData *write_data;
    guint8 header[3] = {0xA5, 0x0, 0x0};
    guint8 tail[1] = {0x5A};
    guint8 checksum = 0;
    guint8 ack = need_ack ? 1 : 0;
    guint i;
    
    if(serial_data==NULL || !serial_data->initialized)
    {
        return;
    }
    
    if(payload==NULL)
    {
        length = 0;
    }
    
    packet = g_byte_array_new();
    g_byte_array_append(packet, header, 3);
    
    packet->data[1] = length + 2;
    packet->data[2] = command;
    
    if(length>0)
    {
        g_byte_array_append(packet, payload, length);
    }
    
    g_byte_array_append(packet, &ack, 1);
    
    for(i=0;i<packet->len;i++)
    {
        checksum ^= packet->data[i];
    }
    g_byte_array_append(packet, &checksum, 1);
    
    g_byte_array_append(packet, tail, 1);
    
    write_data = tl_serial_write_data_new(packet, command, need_ack);
    g_byte_array_unref(packet);
    
    g_queue_push_tail(serial_data->write_queue, write_data);
    
    if(serial_data->write_watch_id==0)
    {
        serial_data->write_watch_id = g_io_add_watch(serial_data->channel,
            G_IO_OUT, tl_serial_write_io_watch_cb, serial_data);
    }
}

static void tl_serial_data_parse(TLSerialData *serial_data)
{
    guint8 command;
    guint8 result;
    
    if(serial_data==NULL)
    {
        return;
    }
    
    if(serial_data->read_size < 5)
    {
        return;
    }
    
    command = serial_data->read_buffer[2];
    result = serial_data->read_buffer[3];
    
    if(serial_data->write_data!=NULL &&
        (serial_data->write_data->command+1)/2==(command+1)/2 &&
        result==0)
    {
        if(serial_data->write_watch_id==0)
        {
            tl_serial_write_data_free(serial_data->write_data);
            serial_data->write_data = NULL;    
        
            if(!g_queue_is_empty(serial_data->write_queue))
            {
                serial_data->write_watch_id = g_io_add_watch(
                    serial_data->channel, G_IO_OUT,
                    tl_serial_write_io_watch_cb, serial_data);
            }
        }
    }
    
    g_message("TLSerial got command %u, result %u.", command, result);
    
    switch(command)
    {
        case 4:
        case 8:
        {
            tl_main_shutdown();
            break;
        }
        case 5:
        {
            serial_data->low_voltage_shutdown = TRUE;
            tl_main_request_shutdown();
            break;
        }
        case 10:
        {
            if(result==0)
            {
                g_message("TLSerial STM8 RTC clock sync finished.");
                serial_data->time_sync_finished = TRUE;
            }
            
            break;
        }
        case 19:
        {
            gint16 x, y, z;
            
            memcpy(&x, serial_data->read_buffer + 3, 2);
            x = g_ntohs(x);
            memcpy(&y, serial_data->read_buffer + 5, 2);
            y = g_ntohs(y);
            memcpy(&z, serial_data->read_buffer + 7, 2);
            z = g_ntohs(z);
            
            g_message("Acceleration changed, x=%hd, y=%hd, z=%hd", x, y, z);
            
            break;
        }
        default:
        {
            break;
        }
    }
    
}

static gboolean tl_serial_read_io_watch_cb(GIOChannel *source,
    GIOCondition condition, gpointer user_data)
{
    TLSerialData *serial_data = (TLSerialData *)user_data;
    ssize_t rsize;
    guint8 buffer[64];
    gint i;
    guint8 checksum, rchecksum;
    guint j;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    if(condition | G_IO_IN)
    {
        while((rsize=read(serial_data->fd, buffer, 64))>0)
        {
            for(i=0;i<rsize;i++)
            {
                if(serial_data->read_size>0)
                {
                    if(serial_data->read_expect_size>0)
                    {
                        serial_data->read_buffer[serial_data->read_size] =
                            buffer[i];
                        serial_data->read_size++;
                        
                        if(serial_data->read_expect_size+4 <=
                            serial_data->read_size)
                        {
                            G_STMT_START
                            {
                                if(serial_data->read_buffer[3+
                                    serial_data->read_expect_size]!=0x5A)
                                {
                                    g_warning("TLSerial received data with "
                                        "incorrect end!");
                                    serial_data->read_size = 0;
                                    serial_data->read_expect_size = 0;
                                    break;
                                }
                                checksum = serial_data->read_buffer[2+
                                    serial_data->read_expect_size];
                                
                                rchecksum = 0;
                                for(j=0;j<serial_data->read_expect_size+2;j++)
                                {
                                    rchecksum ^= serial_data->read_buffer[j];
                                }
                                
                                if(checksum!=rchecksum)
                                {
                                    g_warning("TLSerial received data with "
                                        "incorrect checksum, received %02X, "
                                        "should be %02X", rchecksum, checksum);
                                    serial_data->read_size = 0;
                                    serial_data->read_expect_size = 0;
                                    break;
                                }
                                
                                tl_serial_data_parse(serial_data);
                                serial_data->read_size = 0;
                                serial_data->read_expect_size = 0;
                            }
                            G_STMT_END;
                        }
                    }
                    else
                    {
                        serial_data->read_expect_size = buffer[i];
                        serial_data->read_buffer[1] = buffer[i];
                        serial_data->read_size = 2;
                    }
                }
                else
                {
                    if(buffer[i]==0xA5)
                    {
                        serial_data->read_buffer[0] = 0xA5;
                        serial_data->read_size = 1;
                        serial_data->read_expect_size = 0;
                    }
                }
            }
        } 
    }
    
    return TRUE;
}

static void tl_serial_heartbeat_request(TLSerialData *serial_data)
{
    tl_serial_write_data_request(serial_data, 1, NULL, 0, FALSE);
    serial_data->heartbeat_timestamp = g_get_monotonic_time();
}

static void tl_serial_time_sync_request(TLSerialData *serial_data)
{
    GDateTime *datetime;
    guint8 buffer[7];
    
    if(serial_data->time_sync_finished)
    {
        return;
    }
    
    datetime = g_date_time_new_now_local();
    
    if(g_date_time_get_year(datetime)<2017)
    {
        g_date_time_unref(datetime);
        return;
    }
    
    buffer[0] = (g_date_time_get_year(datetime) - 2000);
    buffer[1] = g_date_time_get_month(datetime);
    buffer[2] = g_date_time_get_day_of_month(datetime);
    buffer[3] = g_date_time_get_day_of_week(datetime);
    buffer[4] = g_date_time_get_hour(datetime);
    buffer[5] = g_date_time_get_minute(datetime);
    buffer[6] = g_date_time_get_second(datetime);
    
    tl_serial_write_data_request(serial_data, 9, buffer, 7, TRUE);
    serial_data->time_sync_timestamp = g_get_monotonic_time();
    
    g_date_time_unref(datetime);
}

static gboolean tl_serial_check_timeout_cb(gpointer user_data)
{
    TLSerialData *serial_data = (TLSerialData *)user_data;
    gint64 now;
    static guint count = 0;
    GDateTime *dt;
    
    if(serial_data->write_watch_id==0 && serial_data->write_data!=NULL)
    {
        now = g_get_monotonic_time();
        
        if(now - serial_data->write_data->timestamp >
            (gint64)TL_SERIAL_RETRY_TIMEOUT * 1e6)
        {
            if(serial_data->write_data->retry_count==0)
            {
                g_warning("TLSerial command %u out of retry count!",
                    serial_data->write_data->command);
                tl_serial_write_data_free(serial_data->write_data);
                serial_data->write_data = NULL;    
            }
            else
            {
                serial_data->write_data->retry_count--;
                serial_data->write_watch_id = g_io_add_watch(
                    serial_data->channel, G_IO_OUT,
                    tl_serial_write_io_watch_cb, serial_data);
            }
        }
    }
    
    if(serial_data->write_watch_id==0 && serial_data->write_data==NULL &&
        !g_queue_is_empty(serial_data->write_queue))
    {
        serial_data->write_watch_id = g_io_add_watch(
            serial_data->channel, G_IO_OUT,
            tl_serial_write_io_watch_cb, serial_data);
    }
    
    if(g_queue_is_empty(serial_data->write_queue))
    {
        now = g_get_monotonic_time();
        if(now - serial_data->heartbeat_timestamp >
            (gint64)TL_SERIAL_HEARTBEAT_TIMEOUT * 1e6)
        {
            tl_serial_heartbeat_request(serial_data);
        }
        
        if(!serial_data->time_sync_finished &&
            now - serial_data->time_sync_timestamp >
            (gint64)TL_SERIAL_TIME_SYNC_TIMEOUT * 1e6)
        {
            tl_serial_time_sync_request(serial_data);
        }
    }
    
    if(serial_data->alarm_clock_enabled && count%600==0)
    {
        dt = g_date_time_new_now_local();
        if(g_date_time_to_unix(dt) > serial_data->alarm_clock_time)
        {
            tl_serial_power_on_daily_set(serial_data->daily_alarm_clock_hour,
                serial_data->daily_alarm_clock_min);
        }
        g_date_time_unref(dt);
    }
    
    count++;
    
    return TRUE;
}

gboolean tl_serial_init(const gchar *port)
{
    int fd;
    GIOChannel *channel;
    struct termios options;
    
    if(g_tl_serial_data.initialized)
    {
        g_warning("TLSerial already initialized!");
        return TRUE;
    }
    
    signal(SIGPIPE, SIG_IGN);
    fd = open(port, O_RDWR | O_NOCTTY | O_NDELAY);
    if(fd<0)
    {
        g_warning("TLSerial cannot open serial port: %s", strerror(errno));
        return FALSE;
    }
    tcgetattr(fd, &options);
    cfmakeraw(&options);
    cfsetispeed(&options, B9600);
    cfsetospeed(&options, B9600);
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
    
    channel = g_io_channel_unix_new(fd);
    if(channel==NULL)
    {
        g_warning("TLSerial cannot open channel for serial port!");
        close(fd);
        return FALSE;
    }
    g_io_channel_set_flags(channel, G_IO_FLAG_NONBLOCK, NULL);
    
    g_tl_serial_data.write_queue = g_queue_new();
    
    g_tl_serial_data.fd = fd;
    g_tl_serial_data.channel = channel;
    g_tl_serial_data.initialized = TRUE;
    g_tl_serial_data.read_expect_size = 0;
    g_tl_serial_data.read_size = 0;
    g_tl_serial_data.write_data = NULL;
    g_tl_serial_data.write_size = 0;
    g_tl_serial_data.write_watch_id = 0;
    
    g_tl_serial_data.read_watch_id =
        g_io_add_watch(channel, G_IO_IN, tl_serial_read_io_watch_cb,
        &g_tl_serial_data);
    
    g_tl_serial_data.check_timeout_id = 
        g_timeout_add(100, tl_serial_check_timeout_cb, &g_tl_serial_data);
        
        
    tl_serial_heartbeat_request(&g_tl_serial_data);
    tl_serial_time_sync_request(&g_tl_serial_data);
    
    if(g_tl_serial_data.gravity_threshold>0)
    {
        tl_serial_gravity_threshold_set(g_tl_serial_data.gravity_threshold);
    }
    
    
    if(g_tl_serial_data.alarm_clock_enabled)
    {
        tl_serial_power_on_time_set(g_tl_serial_data.alarm_clock_time);
    }
    
    return TRUE;
}

void tl_serial_uninit()
{
    if(!g_tl_serial_data.initialized)
    {
        return;
    }
    
    if(g_tl_serial_data.check_timeout_id>0)
    {
        g_source_remove(g_tl_serial_data.check_timeout_id);
        g_tl_serial_data.check_timeout_id = 0;
    }
    
    if(g_tl_serial_data.write_watch_id>0)
    {
        g_source_remove(g_tl_serial_data.write_watch_id);
        g_tl_serial_data.write_watch_id = 0;
    }
    
    if(g_tl_serial_data.read_watch_id>0)
    {
        g_source_remove(g_tl_serial_data.read_watch_id);
        g_tl_serial_data.read_watch_id = 0;
    }
    
    if(g_tl_serial_data.channel!=NULL)
    {
        g_io_channel_unref(g_tl_serial_data.channel);
        g_tl_serial_data.channel = NULL;
    }
    
    if(g_tl_serial_data.fd > 0)
    {
        close(g_tl_serial_data.fd);
        g_tl_serial_data.fd = 0;
    }
    
    if(g_tl_serial_data.write_data!=NULL)
    {
        tl_serial_write_data_free(g_tl_serial_data.write_data);
        g_tl_serial_data.write_data = NULL;
    }
    
    if(g_tl_serial_data.write_queue!=NULL)
    {
        g_queue_free_full(g_tl_serial_data.write_queue, (GDestroyNotify)
            tl_serial_write_data_free);
    }
    g_tl_serial_data.alarm_clock_enabled = FALSE;
    
    g_tl_serial_data.initialized = FALSE;
}

void tl_serial_request_shutdown()
{
    if(!g_tl_serial_data.initialized)
    {
        return;
    }
    
    if(g_tl_serial_data.low_voltage_shutdown)
    {
        tl_serial_write_data_request(&g_tl_serial_data, 7, NULL, 0, TRUE);
    }
    else
    {
        tl_serial_write_data_request(&g_tl_serial_data, 3, NULL, 0, TRUE);
    }
}

void tl_serial_power_on_time_set(gint64 time)
{
    GDateTime *datetime;
    guint8 buffer[7];
    
    g_tl_serial_data.alarm_clock_time = time;
    g_tl_serial_data.alarm_clock_enabled = TRUE;
    
    if(!g_tl_serial_data.initialized)
    {
        return;
    }
    
    datetime = g_date_time_new_from_unix_local(time);
    
    buffer[0] = (g_date_time_get_year(datetime) - 2000);
    buffer[1] = g_date_time_get_month(datetime);
    buffer[2] = g_date_time_get_day_of_month(datetime);
    buffer[3] = g_date_time_get_day_of_week(datetime);
    buffer[4] = g_date_time_get_hour(datetime);
    buffer[5] = g_date_time_get_minute(datetime);
    buffer[6] = g_date_time_get_second(datetime);
    
    tl_serial_write_data_request(&g_tl_serial_data, 11, buffer, 7, TRUE);

    g_date_time_unref(datetime);
    
}

void tl_serial_power_on_daily_set(gint hour, gint minute)
{
    GDateTime *dt;
    gint y, m, d;
    
    if(hour<0 || hour>23)
    {
        return;
    }
    
    g_tl_serial_data.daily_alarm_clock_hour = hour;
    g_tl_serial_data.daily_alarm_clock_min = minute;
    
    dt = g_date_time_new_now_local();
    g_date_time_get_ymd(dt, &y, &m, &d);
    if(g_date_time_get_hour(dt) * 60 + g_date_time_get_minute(dt) >=
        hour*60+minute)
    {
        d++;
    }
    g_date_time_unref(dt);
    dt = g_date_time_new_local(y, m, d, hour, minute, 0);
    tl_serial_power_on_time_set(g_date_time_to_unix(dt));
    g_date_time_unref(dt);
}

void tl_serial_gravity_threshold_set(guint8 threshold)
{
    g_tl_serial_data.gravity_threshold = threshold;
    tl_serial_write_data_request(&g_tl_serial_data, 15, &threshold, 1, TRUE);
}
