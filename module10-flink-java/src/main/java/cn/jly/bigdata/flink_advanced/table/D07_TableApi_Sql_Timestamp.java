package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Set;


/**
 * @author jilanyang
 * @createTime
 */
public class D07_TableApi_Sql_Timestamp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        Set<String> availableZoneIds = ZoneId.getAvailableZoneIds();
        for (String availableZoneId : availableZoneIds) {
            System.out.println("availableZoneId = " + availableZoneId);
        }

        // 创建流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tableEnv.executeSql("CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3)");
        Table table1 = tableEnv.sqlQuery("SELECT * FROM T1");

        // 转换为流并打印输入: +I[1970-01-01T00:00:04.001Z]
        DataStream<Row> resDS1 = tableEnv.toAppendStream(table1, Row.class);
        resDS1.print("resDS1");

        /*
            以下时间功能受配置时区的影响。  -- 本示例中本地（上海）和UTC时间相比差8小时
            LOCALTIME - 本地时间（根据自己设置的时区来定，本示例是上海）
            LOCALTIMESTAMP - 本地时间戳（日期+时间）
            CURRENT_DATE - 当前日期(本地日期)
            CURRENT_TIME - 当前时间（本地时间）
            CURRENT_TIMESTAMP - 当前时间戳（UTC日期+时间）
            CURRENT_ROW_TIMESTAMP() - 当前行时间戳（UTC日期+时间）
            NOW() - 现在时间（UTC日期+时间）
            PROCTIME() - 处理时间（UTC日期+时间）
         */
        tableEnv.executeSql("CREATE VIEW MyView1 AS SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME," +
                " CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME()");
        Table table2 = tableEnv.sqlQuery("SELECT * FROM MyView1");
        table2.printSchema();
        DataStream<Row> resDS2 = tableEnv.toAppendStream(table2, Row.class);
        resDS2.print("resDS2");

        // 会话时区用于将timestamp_LTZ值表示为字符串格式，即打印值、将值转换为字符串类型、将值转换为时间戳、将时间戳值转换为timestamp_LTZ：
        tableEnv.executeSql("CREATE VIEW MyView2 AS SELECT TO_TIMESTAMP_LTZ(4001, 3) AS ltz, TIMESTAMP '1970-01-01 00:00:01.001'  AS ntz");
        Table table3 = tableEnv.sqlQuery("select * from MyView2");
        table3.printSchema();
        DataStream<Row> resDS3 = tableEnv.toAppendStream(table3, Row.class);
        resDS3.print("resDS3");

        env.execute("D07_TableApi_Sql_Timestamp");

        /*
        附录：可以用的时区id
        availableZoneId = Asia/Aden
availableZoneId = America/Cuiaba
availableZoneId = Etc/GMT+9
availableZoneId = Etc/GMT+8
availableZoneId = Africa/Nairobi
availableZoneId = America/Marigot
availableZoneId = Asia/Aqtau
availableZoneId = Pacific/Kwajalein
availableZoneId = America/El_Salvador
availableZoneId = Asia/Pontianak
availableZoneId = Africa/Cairo
availableZoneId = Pacific/Pago_Pago
availableZoneId = Africa/Mbabane
availableZoneId = Asia/Kuching
availableZoneId = Pacific/Honolulu
availableZoneId = Pacific/Rarotonga
availableZoneId = America/Guatemala
availableZoneId = Australia/Hobart
availableZoneId = Europe/London
availableZoneId = America/Belize
availableZoneId = America/Panama
availableZoneId = Asia/Chungking
availableZoneId = America/Managua
availableZoneId = America/Indiana/Petersburg
availableZoneId = Asia/Yerevan
availableZoneId = Europe/Brussels
availableZoneId = GMT
availableZoneId = Europe/Warsaw
availableZoneId = America/Chicago
availableZoneId = Asia/Kashgar
availableZoneId = Chile/Continental
availableZoneId = Pacific/Yap
availableZoneId = CET
availableZoneId = Etc/GMT-1
availableZoneId = Etc/GMT-0
availableZoneId = Europe/Jersey
availableZoneId = America/Tegucigalpa
availableZoneId = Etc/GMT-5
availableZoneId = Europe/Istanbul
availableZoneId = America/Eirunepe
availableZoneId = Etc/GMT-4
availableZoneId = America/Miquelon
availableZoneId = Etc/GMT-3
availableZoneId = Europe/Luxembourg
availableZoneId = Etc/GMT-2
availableZoneId = Etc/GMT-9
availableZoneId = America/Argentina/Catamarca
availableZoneId = Etc/GMT-8
availableZoneId = Etc/GMT-7
availableZoneId = Etc/GMT-6
availableZoneId = Europe/Zaporozhye
availableZoneId = Canada/Yukon
availableZoneId = Canada/Atlantic
availableZoneId = Atlantic/St_Helena
availableZoneId = Australia/Tasmania
availableZoneId = Libya
availableZoneId = Europe/Guernsey
availableZoneId = America/Grand_Turk
availableZoneId = US/Pacific-New
availableZoneId = Asia/Samarkand
availableZoneId = America/Argentina/Cordoba
availableZoneId = Asia/Phnom_Penh
availableZoneId = Africa/Kigali
availableZoneId = Asia/Almaty
availableZoneId = US/Alaska
availableZoneId = Asia/Dubai
availableZoneId = Europe/Isle_of_Man
availableZoneId = America/Araguaina
availableZoneId = Cuba
availableZoneId = Asia/Novosibirsk
availableZoneId = America/Argentina/Salta
availableZoneId = Etc/GMT+3
availableZoneId = Africa/Tunis
availableZoneId = Etc/GMT+2
availableZoneId = Etc/GMT+1
availableZoneId = Pacific/Fakaofo
availableZoneId = Africa/Tripoli
availableZoneId = Etc/GMT+0
availableZoneId = Israel
availableZoneId = Africa/Banjul
availableZoneId = Etc/GMT+7
availableZoneId = Indian/Comoro
availableZoneId = Etc/GMT+6
availableZoneId = Etc/GMT+5
availableZoneId = Etc/GMT+4
availableZoneId = Pacific/Port_Moresby
availableZoneId = US/Arizona
availableZoneId = Antarctica/Syowa
availableZoneId = Indian/Reunion
availableZoneId = Pacific/Palau
availableZoneId = Europe/Kaliningrad
availableZoneId = America/Montevideo
availableZoneId = Africa/Windhoek
availableZoneId = Asia/Karachi
availableZoneId = Africa/Mogadishu
availableZoneId = Australia/Perth
availableZoneId = Brazil/East
availableZoneId = Etc/GMT
availableZoneId = Asia/Chita
availableZoneId = Pacific/Easter
availableZoneId = Antarctica/Davis
availableZoneId = Antarctica/McMurdo
availableZoneId = Asia/Macao
availableZoneId = America/Manaus
availableZoneId = Africa/Freetown
availableZoneId = Europe/Bucharest
availableZoneId = Asia/Tomsk
availableZoneId = America/Argentina/Mendoza
availableZoneId = Asia/Macau
availableZoneId = Europe/Malta
availableZoneId = Mexico/BajaSur
availableZoneId = Pacific/Tahiti
availableZoneId = Africa/Asmera
availableZoneId = Europe/Busingen
availableZoneId = America/Argentina/Rio_Gallegos
availableZoneId = Africa/Malabo
availableZoneId = Europe/Skopje
availableZoneId = America/Catamarca
availableZoneId = America/Godthab
availableZoneId = Europe/Sarajevo
availableZoneId = Australia/ACT
availableZoneId = GB-Eire
availableZoneId = Africa/Lagos
availableZoneId = America/Cordoba
availableZoneId = Europe/Rome
availableZoneId = Asia/Dacca
availableZoneId = Indian/Mauritius
availableZoneId = Pacific/Samoa
availableZoneId = America/Regina
availableZoneId = America/Fort_Wayne
availableZoneId = America/Dawson_Creek
availableZoneId = Africa/Algiers
availableZoneId = Europe/Mariehamn
availableZoneId = America/St_Johns
availableZoneId = America/St_Thomas
availableZoneId = Europe/Zurich
availableZoneId = America/Anguilla
availableZoneId = Asia/Dili
availableZoneId = America/Denver
availableZoneId = Africa/Bamako
availableZoneId = GB
availableZoneId = Mexico/General
availableZoneId = Pacific/Wallis
availableZoneId = Europe/Gibraltar
availableZoneId = Africa/Conakry
availableZoneId = Africa/Lubumbashi
availableZoneId = Asia/Istanbul
availableZoneId = America/Havana
availableZoneId = NZ-CHAT
availableZoneId = Asia/Choibalsan
availableZoneId = America/Porto_Acre
availableZoneId = Asia/Omsk
availableZoneId = Europe/Vaduz
availableZoneId = US/Michigan
availableZoneId = Asia/Dhaka
availableZoneId = America/Barbados
availableZoneId = Europe/Tiraspol
availableZoneId = Atlantic/Cape_Verde
availableZoneId = Asia/Yekaterinburg
availableZoneId = America/Louisville
availableZoneId = Pacific/Johnston
availableZoneId = Pacific/Chatham
availableZoneId = Europe/Ljubljana
availableZoneId = America/Sao_Paulo
availableZoneId = Asia/Jayapura
availableZoneId = America/Curacao
availableZoneId = Asia/Dushanbe
availableZoneId = America/Guyana
availableZoneId = America/Guayaquil
availableZoneId = America/Martinique
availableZoneId = Portugal
availableZoneId = Europe/Berlin
availableZoneId = Europe/Moscow
availableZoneId = Europe/Chisinau
availableZoneId = America/Puerto_Rico
availableZoneId = America/Rankin_Inlet
availableZoneId = Pacific/Ponape
availableZoneId = Europe/Stockholm
availableZoneId = Europe/Budapest
availableZoneId = America/Argentina/Jujuy
availableZoneId = Australia/Eucla
availableZoneId = Asia/Shanghai
availableZoneId = Universal
availableZoneId = Europe/Zagreb
availableZoneId = America/Port_of_Spain
availableZoneId = Europe/Helsinki
availableZoneId = Asia/Beirut
availableZoneId = Asia/Tel_Aviv
availableZoneId = Pacific/Bougainville
availableZoneId = US/Central
availableZoneId = Africa/Sao_Tome
availableZoneId = Indian/Chagos
availableZoneId = America/Cayenne
availableZoneId = Asia/Yakutsk
availableZoneId = Pacific/Galapagos
availableZoneId = Australia/North
availableZoneId = Europe/Paris
availableZoneId = Africa/Ndjamena
availableZoneId = Pacific/Fiji
availableZoneId = America/Rainy_River
availableZoneId = Indian/Maldives
availableZoneId = Australia/Yancowinna
availableZoneId = SystemV/AST4
availableZoneId = Asia/Oral
availableZoneId = America/Yellowknife
availableZoneId = Pacific/Enderbury
availableZoneId = America/Juneau
availableZoneId = Australia/Victoria
availableZoneId = America/Indiana/Vevay
availableZoneId = Asia/Tashkent
availableZoneId = Asia/Jakarta
availableZoneId = Africa/Ceuta
availableZoneId = Asia/Barnaul
availableZoneId = America/Recife
availableZoneId = America/Buenos_Aires
availableZoneId = America/Noronha
availableZoneId = America/Swift_Current
availableZoneId = Australia/Adelaide
availableZoneId = America/Metlakatla
availableZoneId = Africa/Djibouti
availableZoneId = America/Paramaribo
availableZoneId = Europe/Simferopol
availableZoneId = Europe/Sofia
availableZoneId = Africa/Nouakchott
availableZoneId = Europe/Prague
availableZoneId = America/Indiana/Vincennes
availableZoneId = Antarctica/Mawson
availableZoneId = America/Kralendijk
availableZoneId = Antarctica/Troll
availableZoneId = Europe/Samara
availableZoneId = Indian/Christmas
availableZoneId = America/Antigua
availableZoneId = Pacific/Gambier
availableZoneId = America/Indianapolis
availableZoneId = America/Inuvik
availableZoneId = America/Iqaluit
availableZoneId = Pacific/Funafuti
availableZoneId = UTC
availableZoneId = Antarctica/Macquarie
availableZoneId = Canada/Pacific
availableZoneId = America/Moncton
availableZoneId = Africa/Gaborone
availableZoneId = Pacific/Chuuk
availableZoneId = Asia/Pyongyang
availableZoneId = America/St_Vincent
availableZoneId = Asia/Gaza
availableZoneId = Etc/Universal
availableZoneId = PST8PDT
availableZoneId = Atlantic/Faeroe
availableZoneId = Asia/Qyzylorda
availableZoneId = Canada/Newfoundland
availableZoneId = America/Kentucky/Louisville
availableZoneId = America/Yakutat
availableZoneId = Asia/Ho_Chi_Minh
availableZoneId = Antarctica/Casey
availableZoneId = Europe/Copenhagen
availableZoneId = Africa/Asmara
availableZoneId = Atlantic/Azores
availableZoneId = Europe/Vienna
availableZoneId = ROK
availableZoneId = Pacific/Pitcairn
availableZoneId = America/Mazatlan
availableZoneId = Australia/Queensland
availableZoneId = Pacific/Nauru
availableZoneId = Europe/Tirane
availableZoneId = Asia/Kolkata
availableZoneId = SystemV/MST7
availableZoneId = Australia/Canberra
availableZoneId = MET
availableZoneId = Australia/Broken_Hill
availableZoneId = Europe/Riga
availableZoneId = America/Dominica
availableZoneId = Africa/Abidjan
availableZoneId = America/Mendoza
availableZoneId = America/Santarem
availableZoneId = Kwajalein
availableZoneId = America/Asuncion
availableZoneId = Asia/Ulan_Bator
availableZoneId = NZ
availableZoneId = America/Boise
availableZoneId = Australia/Currie
availableZoneId = EST5EDT
availableZoneId = Pacific/Guam
availableZoneId = Pacific/Wake
availableZoneId = Atlantic/Bermuda
availableZoneId = America/Costa_Rica
availableZoneId = America/Dawson
availableZoneId = Asia/Chongqing
availableZoneId = Eire
availableZoneId = Europe/Amsterdam
availableZoneId = America/Indiana/Knox
availableZoneId = America/North_Dakota/Beulah
availableZoneId = Africa/Accra
availableZoneId = Atlantic/Faroe
availableZoneId = Mexico/BajaNorte
availableZoneId = America/Maceio
availableZoneId = Etc/UCT
availableZoneId = Pacific/Apia
availableZoneId = GMT0
availableZoneId = America/Atka
availableZoneId = Pacific/Niue
availableZoneId = Canada/East-Saskatchewan
availableZoneId = Australia/Lord_Howe
availableZoneId = Europe/Dublin
availableZoneId = Pacific/Truk
availableZoneId = MST7MDT
availableZoneId = America/Monterrey
availableZoneId = America/Nassau
availableZoneId = America/Jamaica
availableZoneId = Asia/Bishkek
availableZoneId = America/Atikokan
availableZoneId = Atlantic/Stanley
availableZoneId = Australia/NSW
availableZoneId = US/Hawaii
availableZoneId = SystemV/CST6
availableZoneId = Indian/Mahe
availableZoneId = Asia/Aqtobe
availableZoneId = America/Sitka
availableZoneId = Asia/Vladivostok
availableZoneId = Africa/Libreville
availableZoneId = Africa/Maputo
availableZoneId = Zulu
availableZoneId = America/Kentucky/Monticello
availableZoneId = Africa/El_Aaiun
availableZoneId = Africa/Ouagadougou
availableZoneId = America/Coral_Harbour
availableZoneId = Pacific/Marquesas
availableZoneId = Brazil/West
availableZoneId = America/Aruba
availableZoneId = America/North_Dakota/Center
availableZoneId = America/Cayman
availableZoneId = Asia/Ulaanbaatar
availableZoneId = Asia/Baghdad
availableZoneId = Europe/San_Marino
availableZoneId = America/Indiana/Tell_City
availableZoneId = America/Tijuana
availableZoneId = Pacific/Saipan
availableZoneId = SystemV/YST9
availableZoneId = Africa/Douala
availableZoneId = America/Chihuahua
availableZoneId = America/Ojinaga
availableZoneId = Asia/Hovd
availableZoneId = America/Anchorage
availableZoneId = Chile/EasterIsland
availableZoneId = America/Halifax
availableZoneId = Antarctica/Rothera
availableZoneId = America/Indiana/Indianapolis
availableZoneId = US/Mountain
availableZoneId = Asia/Damascus
availableZoneId = America/Argentina/San_Luis
availableZoneId = America/Santiago
availableZoneId = Asia/Baku
availableZoneId = America/Argentina/Ushuaia
availableZoneId = Atlantic/Reykjavik
availableZoneId = Africa/Brazzaville
availableZoneId = Africa/Porto-Novo
availableZoneId = America/La_Paz
availableZoneId = Antarctica/DumontDUrville
availableZoneId = Asia/Taipei
availableZoneId = Antarctica/South_Pole
availableZoneId = Asia/Manila
availableZoneId = Asia/Bangkok
availableZoneId = Africa/Dar_es_Salaam
availableZoneId = Poland
availableZoneId = Atlantic/Madeira
availableZoneId = Antarctica/Palmer
availableZoneId = America/Thunder_Bay
availableZoneId = Africa/Addis_Ababa
availableZoneId = Europe/Uzhgorod
availableZoneId = Brazil/DeNoronha
availableZoneId = Asia/Ashkhabad
availableZoneId = Etc/Zulu
availableZoneId = America/Indiana/Marengo
availableZoneId = America/Creston
availableZoneId = America/Mexico_City
availableZoneId = Antarctica/Vostok
availableZoneId = Asia/Jerusalem
availableZoneId = Europe/Andorra
availableZoneId = US/Samoa
availableZoneId = PRC
availableZoneId = Asia/Vientiane
availableZoneId = Pacific/Kiritimati
availableZoneId = America/Matamoros
availableZoneId = America/Blanc-Sablon
availableZoneId = Asia/Riyadh
availableZoneId = Iceland
availableZoneId = Pacific/Pohnpei
availableZoneId = Asia/Ujung_Pandang
availableZoneId = Atlantic/South_Georgia
availableZoneId = Europe/Lisbon
availableZoneId = Asia/Harbin
availableZoneId = Europe/Oslo
availableZoneId = Asia/Novokuznetsk
availableZoneId = CST6CDT
availableZoneId = Atlantic/Canary
availableZoneId = America/Knox_IN
availableZoneId = Asia/Kuwait
availableZoneId = SystemV/HST10
availableZoneId = Pacific/Efate
availableZoneId = Africa/Lome
availableZoneId = America/Bogota
availableZoneId = America/Menominee
availableZoneId = America/Adak
availableZoneId = Pacific/Norfolk
availableZoneId = Europe/Kirov
availableZoneId = America/Resolute
availableZoneId = Pacific/Tarawa
availableZoneId = Africa/Kampala
availableZoneId = Asia/Krasnoyarsk
availableZoneId = Greenwich
availableZoneId = SystemV/EST5
availableZoneId = America/Edmonton
availableZoneId = Europe/Podgorica
availableZoneId = Australia/South
availableZoneId = Canada/Central
availableZoneId = Africa/Bujumbura
availableZoneId = America/Santo_Domingo
availableZoneId = US/Eastern
availableZoneId = Europe/Minsk
availableZoneId = Pacific/Auckland
availableZoneId = Africa/Casablanca
availableZoneId = America/Glace_Bay
availableZoneId = Canada/Eastern
availableZoneId = Asia/Qatar
availableZoneId = Europe/Kiev
availableZoneId = Singapore
availableZoneId = Asia/Magadan
availableZoneId = SystemV/PST8
availableZoneId = America/Port-au-Prince
availableZoneId = Europe/Belfast
availableZoneId = America/St_Barthelemy
availableZoneId = Asia/Ashgabat
availableZoneId = Africa/Luanda
availableZoneId = America/Nipigon
availableZoneId = Atlantic/Jan_Mayen
availableZoneId = Brazil/Acre
availableZoneId = Asia/Muscat
availableZoneId = Asia/Bahrain
availableZoneId = Europe/Vilnius
availableZoneId = America/Fortaleza
availableZoneId = Etc/GMT0
availableZoneId = US/East-Indiana
availableZoneId = America/Hermosillo
availableZoneId = America/Cancun
availableZoneId = Africa/Maseru
availableZoneId = Pacific/Kosrae
availableZoneId = Africa/Kinshasa
availableZoneId = Asia/Kathmandu
availableZoneId = Asia/Seoul
availableZoneId = Australia/Sydney
availableZoneId = America/Lima
availableZoneId = Australia/LHI
availableZoneId = America/St_Lucia
availableZoneId = Europe/Madrid
availableZoneId = America/Bahia_Banderas
availableZoneId = America/Montserrat
availableZoneId = Asia/Brunei
availableZoneId = America/Santa_Isabel
availableZoneId = Canada/Mountain
availableZoneId = America/Cambridge_Bay
availableZoneId = Asia/Colombo
availableZoneId = Australia/West
availableZoneId = Indian/Antananarivo
availableZoneId = Australia/Brisbane
availableZoneId = Indian/Mayotte
availableZoneId = US/Indiana-Starke
availableZoneId = Asia/Urumqi
availableZoneId = US/Aleutian
availableZoneId = Europe/Volgograd
availableZoneId = America/Lower_Princes
availableZoneId = America/Vancouver
availableZoneId = Africa/Blantyre
availableZoneId = America/Rio_Branco
availableZoneId = America/Danmarkshavn
availableZoneId = America/Detroit
availableZoneId = America/Thule
availableZoneId = Africa/Lusaka
availableZoneId = Asia/Hong_Kong
availableZoneId = Iran
availableZoneId = America/Argentina/La_Rioja
availableZoneId = Africa/Dakar
availableZoneId = SystemV/CST6CDT
availableZoneId = America/Tortola
availableZoneId = America/Porto_Velho
availableZoneId = Asia/Sakhalin
availableZoneId = Etc/GMT+10
availableZoneId = America/Scoresbysund
availableZoneId = Asia/Kamchatka
availableZoneId = Asia/Thimbu
availableZoneId = Africa/Harare
availableZoneId = Etc/GMT+12
availableZoneId = Etc/GMT+11
availableZoneId = Navajo
availableZoneId = America/Nome
availableZoneId = Europe/Tallinn
availableZoneId = Turkey
availableZoneId = Africa/Khartoum
availableZoneId = Africa/Johannesburg
availableZoneId = Africa/Bangui
availableZoneId = Europe/Belgrade
availableZoneId = Jamaica
availableZoneId = Africa/Bissau
availableZoneId = Asia/Tehran
availableZoneId = WET
availableZoneId = Europe/Astrakhan
availableZoneId = Africa/Juba
availableZoneId = America/Campo_Grande
availableZoneId = America/Belem
availableZoneId = Etc/Greenwich
availableZoneId = Asia/Saigon
availableZoneId = America/Ensenada
availableZoneId = Pacific/Midway
availableZoneId = America/Jujuy
availableZoneId = Africa/Timbuktu
availableZoneId = America/Bahia
availableZoneId = America/Goose_Bay
availableZoneId = America/Virgin
availableZoneId = America/Pangnirtung
availableZoneId = Asia/Katmandu
availableZoneId = America/Phoenix
availableZoneId = Africa/Niamey
availableZoneId = America/Whitehorse
availableZoneId = Pacific/Noumea
availableZoneId = Asia/Tbilisi
availableZoneId = America/Montreal
availableZoneId = Asia/Makassar
availableZoneId = America/Argentina/San_Juan
availableZoneId = Hongkong
availableZoneId = UCT
availableZoneId = Asia/Nicosia
availableZoneId = America/Indiana/Winamac
availableZoneId = SystemV/MST7MDT
availableZoneId = America/Argentina/ComodRivadavia
availableZoneId = America/Boa_Vista
availableZoneId = America/Grenada
availableZoneId = Australia/Darwin
availableZoneId = Asia/Khandyga
availableZoneId = Asia/Kuala_Lumpur
availableZoneId = Asia/Thimphu
availableZoneId = Asia/Rangoon
availableZoneId = Europe/Bratislava
availableZoneId = Asia/Calcutta
availableZoneId = America/Argentina/Tucuman
availableZoneId = Asia/Kabul
availableZoneId = Indian/Cocos
availableZoneId = Japan
availableZoneId = Pacific/Tongatapu
availableZoneId = America/New_York
availableZoneId = Etc/GMT-12
availableZoneId = Etc/GMT-11
availableZoneId = Etc/GMT-10
availableZoneId = SystemV/YST9YDT
availableZoneId = Europe/Ulyanovsk
availableZoneId = Etc/GMT-14
availableZoneId = Etc/GMT-13
availableZoneId = W-SU
availableZoneId = America/Merida
availableZoneId = EET
availableZoneId = America/Rosario
availableZoneId = Canada/Saskatchewan
availableZoneId = America/St_Kitts
availableZoneId = Arctic/Longyearbyen
availableZoneId = America/Fort_Nelson
availableZoneId = America/Caracas
availableZoneId = America/Guadeloupe
availableZoneId = Asia/Hebron
availableZoneId = Indian/Kerguelen
availableZoneId = SystemV/PST8PDT
availableZoneId = Africa/Monrovia
availableZoneId = Asia/Ust-Nera
availableZoneId = Egypt
availableZoneId = Asia/Srednekolymsk
availableZoneId = America/North_Dakota/New_Salem
availableZoneId = Asia/Anadyr
availableZoneId = Australia/Melbourne
availableZoneId = Asia/Irkutsk
availableZoneId = America/Shiprock
availableZoneId = America/Winnipeg
availableZoneId = Europe/Vatican
availableZoneId = Asia/Amman
availableZoneId = Etc/UTC
availableZoneId = SystemV/AST4ADT
availableZoneId = Asia/Tokyo
availableZoneId = America/Toronto
availableZoneId = Asia/Singapore
availableZoneId = Australia/Lindeman
availableZoneId = America/Los_Angeles
availableZoneId = SystemV/EST5EDT
availableZoneId = Pacific/Majuro
availableZoneId = America/Argentina/Buenos_Aires
availableZoneId = Europe/Nicosia
availableZoneId = Pacific/Guadalcanal
availableZoneId = Europe/Athens
availableZoneId = US/Pacific
availableZoneId = Europe/Monaco
         */
    }
}