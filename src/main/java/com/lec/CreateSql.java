package com.lec;

import com.lec.common.utils.FIleUtil;
import com.lec.common.utils.JdbcUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateSql {
    public static Map<String, String> idxDicMap;
    public static Map<String, String> diyLablesMap;
    private static final Logger log = Logger.getLogger(CreateSql.class);

    static {
        idxDicMap = JdbcUtil.getIdxDfnMap();
        diyLablesMap = JdbcUtil.getLabDfnMap();
    }

    public static void main(String[] args) {
        if (idxDicMap.size() == 0) {
            log.error("idxDicMap 为空 程序终止");
            return;
        }
        if (diyLablesMap.size() == 0) {
            log.error("diyLablesMap 为空 程序终止");
            return;
        }

        for (Map.Entry<String, String> entry : diyLablesMap.entrySet()) {
            System.out.println(makeSql(entry.getValue(), entry.getKey()));
            try {
                FIleUtil.generateFile(args[0] + File.separatorChar + entry.getKey() + ".sql", makeSql(entry.getValue(), entry.getKey()));
                continue;
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            System.exit(0);
        }


//        String labelId = "标签1";
//        String str = "  (  COR_00014 >= 12 AND COR_00032 < 111 )  AND  (  ( COR_00057 >= 10 AND  COR_00057 <= 10 )  AND ( COR_00019 >= 2021-06-16 AND  COR_00019 <= 2021-07-09 )  )  ";


    }

    public static String makeSql(String str, String labelId) {
        String[] split = str.split(" ");
        Set<String> tableSet = new HashSet<>();
        Set<String> cloumnSet = new HashSet<>();
        for (int i = 0; i < split.length; i++) {
            String cloumn = split[i];
            if (checkId(cloumn)) {
                tableSet.add(idxDicMap.get(cloumn));
                cloumnSet.add(cloumn);
            }
        }

        String sql = diffcult(str, labelId, cloumnSet);
//        String a = "\\$\\{tx_date\\}";
//        return sql.replaceAll(a, "2021-03-31");
        return sql;
    }




    public static String diffcult(String str, String labelId, Set<String> cloumnSet) {


        String head = "select '${hivevar:data_dt}' as data_dt ,'" + labelId + "' as lbl_no ,'0' as lbl_val,org_ecd ,ecif_cust_no ,'" + labelId + "' as etl_task_apltn ,current_timestamp as data_load_tm from ";
        String where = " where " + str + " ;";
        String childSearch = childSearch(cloumnSet);
        return head + childSearch + where;
    }

    public static boolean checkId(String str) {
        String[] patterns = {"^COR_\\d{5}$", "^PRI_\\d{5}$"};
        for (String pattern : patterns) {
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(str);
            if (m.matches() == true) {
                return true;
            }
        }


        return false;
    }


    public static String tempTable(Set cloumnSet) {
        String sql = "";
        Iterator iterator = cloumnSet.iterator();
        while (iterator.hasNext()) {
            String cloumn = (String) iterator.next();
            String tail = sql.equalsIgnoreCase("") ? " " : " union all ";
            sql += tail;
            sql += "select idx_ecd ,idx_val,org_ecd,ecif_cust_no from " + idxDicMap.get(cloumn) + " where data_dt='${hivevar:data_dt}' and " + " idx_ecd='" + cloumn + "'";


        }
        return "(" + sql + ") tmp";
    }

    public static String childSearch(Set cloumnSet) {

        String childSearch = "";
        Iterator iterator = cloumnSet.iterator();
        while (iterator.hasNext()) {
            String cloumn = (String) iterator.next();
            String tail = childSearch.equalsIgnoreCase("") ? " " : " , ";
            childSearch += tail;
            childSearch += " Max(CASE WHEN idx_ecd = '" + cloumn + "' THEN idx_val  ELSE '0' END) AS " + cloumn;

        }
        return "( select org_ecd,ecif_cust_no," + childSearch + " from " + tempTable(cloumnSet) + " group by ecif_cust_no,org_ecd )  child ";
    }


}
