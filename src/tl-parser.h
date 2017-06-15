#ifndef HAVE_TL_PARSER_H
#define HAVE_TL_PARSER_H

#include <glib.h>

typedef struct _TLParserSignalData
{
    int id;
    gchar *name;
    gboolean endian;
    guint firstbyte;
    guint firstbit;
    guint bitlength;
    gdouble unit;
    int offset;
    int source;
}TLParserSignalData;

#define TL_PARSER_VEHICLE_FAULT_LEVEL "VCU01_VehicleFaultLevel"
#define TL_PARSER_VEHICLE_STATE "VCU01_PTReady"
#define TL_PARSER_BATTERY_STATE "BMS01_BatState"
#define TL_PARSER_RUNNING_MODE "BMS01_PTMode"
#define TL_PARSER_VEHICLE_SPEED "VCU08_VehicleSpeed"
#define TL_PARSER_TOTAL_MILEAGE "DPU01_ODO"
#define TL_PARSER_TOTAL_VOLTAGE "BMS01_actVoltage"
#define TL_PARSER_TOTAL_CURRENT "BMS01_actCurrent"
#define TL_PARSER_SOC_STATE "BMS01_actSOC"
#define TL_PARSER_DC2DC_STATE "VCU09_StOpMode"
#define TL_PARSER_GEAR_SHIFT_STATE "VCU01_StGear"
#define TL_PARSER_INSULATION_RESISTANCE "BMS02_IsoResistance"
#define TL_PARSER_ACCELERATOR_LEVEL "VCU03_GasNrm"
#define TL_PARSER_BRAKE_LEVEL "VCU01_bBrk"

#define TL_PARSER_BATTERY_NUMBER "BMS08_BatNumber"

gboolean tl_parser_init();
void tl_parser_uninit();
gboolean tl_parser_load_parse_file(const gchar *file);
gboolean tl_parser_parse_can_data(const gchar *device,
    int can_id, const guint8 *data, gsize len);
const gchar *tl_parser_battery_code_get(guint8 *single_bat_code_len,
    guint *bat_code_total_len);

#endif
