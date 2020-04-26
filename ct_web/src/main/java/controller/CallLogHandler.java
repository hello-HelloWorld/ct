package controller;

import bean.CallLog;
import bean.QueryInfo;
import dao.CallLogDAO;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/24 9:56
 */
@Controller
public class CallLogHandler {

    @RequestMapping("/queryCallLogList")
    public String queryCallLog(Model model, QueryInfo queryInfo) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
        CallLogDAO callLogDAO = applicationContext.getBean(CallLogDAO.class);

        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put("telephone", queryInfo.getTelephone());
        hashMap.put("year", queryInfo.getYear());
        hashMap.put("month", queryInfo.getMonth());
        hashMap.put("day", queryInfo.getDay());

        List<CallLog> callLogList = callLogDAO.getCallLogList(hashMap);

        StringBuilder dateSb = new StringBuilder();
        StringBuilder callSumSb = new StringBuilder();
        StringBuilder callDurationSumSb = new StringBuilder();

        for (int i = 0; i < callLogList.size(); i++) {
            CallLog callLog = callLogList.get(i);
            //1月, 2月, ....12月,
            dateSb.append(callLog.getMonth() + "月");
            callSumSb.append(callLog.getCall_sum() + ",");
            callDurationSumSb.append(callLog.getCall_duration_sum() + ",");
        }

        dateSb.deleteCharAt(dateSb.length() - 1);
        callSumSb.deleteCharAt(callSumSb.length() - 1);
        callDurationSumSb.deleteCharAt(callDurationSumSb.length() - 1);

        //通过model返回数据
        model.addAttribute("telephone", callLogList.get(0).getTelephone());
        model.addAttribute("name", callLogList.get(0).getName());
        model.addAttribute("date", dateSb.toString());
        model.addAttribute("count", callSumSb.toString());
        model.addAttribute("duration", callDurationSumSb.toString());

        return "jsp/CallLogListEchart";
    }
}
