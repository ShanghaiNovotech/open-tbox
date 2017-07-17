#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <gps.h>
#include <math.h>

#include "tl-gps.h"

typedef struct _TLGPSData
{
    gboolean initialized;
    gboolean work_flag;
    GThread *work_thread;
}TLGPSData;

static TLGPSData g_tl_gps_data = {0};

static gpointer tl_gps_work_thread(gpointer user_data)
{
    TLGPSData *gps_data = (TLGPSData *)user_data;
    struct gps_data_t gdata;
    int rc;
    gboolean open_flag = FALSE;
    
    if(user_data==NULL)
    {
        return NULL;
    }
    
    while(gps_data->work_flag)
    {
        if(!open_flag && (rc=gps_open("127.0.0.1", "2947", &gdata))==-1)
        {
            g_warning("TLGPS failed to open GPS: %s", gps_errstr(rc));
            g_usleep(2000000UL);
            continue;
        }
        else
        {
            open_flag = TRUE;
            gps_stream(&gdata, WATCH_ENABLE | WATCH_JSON, NULL);
        }

        if(gps_waiting(&gdata, 500000))
        {
            if((rc=gps_read(&gdata))==-1)
            {
                g_warning("TLGPS failed to read GPS data: %s", gps_errstr(rc));
                gps_stream(&gdata, WATCH_DISABLE, NULL);
                gps_close (&gdata);
                open_flag = FALSE;
            }
            else
            {
                if((gdata.status==STATUS_FIX) && (gdata.fix.mode==MODE_2D ||
                    gdata.fix.mode==MODE_3D) && !isnan(gdata.fix.latitude) && 
                    !isnan(gdata.fix.longitude))
                {
                    g_message("Latitude: %lf, longitude: %lf, speed: %lf, "
                        "timestamp: %lf.", gdata.fix.latitude,
                        gdata.fix.longitude, gdata.fix.speed,
                        gdata.fix.time);
                }
                else
                {
                    g_message("No GPS data available.");
                }
            }
        }
        
        g_usleep(500000UL);
    }
    
    if(open_flag)
    {
        gps_stream(&gdata, WATCH_DISABLE, NULL);
        gps_close (&gdata);
    }

    return NULL;
}

gboolean tl_gps_init()
{
    if(g_tl_gps_data.initialized)
    {
        g_warning("TLGPS already initialized!");
        return TRUE;
    }
    
    g_tl_gps_data.work_thread = g_thread_new("tl-gps-thread",
        tl_gps_work_thread, &g_tl_gps_data);
    
    g_tl_gps_data.initialized = TRUE;
    
    return TRUE;
}

void tl_gps_uninit()
{
    if(!g_tl_gps_data.initialized)
    {
        return;
    }
    
    if(g_tl_gps_data.work_thread!=NULL)
    {
        g_tl_gps_data.work_flag = FALSE;
        g_thread_join(g_tl_gps_data.work_thread);
        g_tl_gps_data.work_thread = NULL;
    }

    g_tl_gps_data.initialized = FALSE;
}
