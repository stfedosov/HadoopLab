import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author sfedosov on 12/27/18.
 */
public class DateInterceptor implements Interceptor {
    private static final String TIMESTAMP_FORMAT = "yyyy-dd-MM hh:mm:ss";

    public DateInterceptor() {
    }

    public void initialize() {
    }

    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String bodyS = (new String(body)).trim();
        if(bodyS.isEmpty()) {
            return event;
        } else {
            String[] splitted = bodyS.split(",");
            String date = splitted[2];
            SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT);

            try {
                Date parse = sdf.parse(date);
                Calendar c = Calendar.getInstance();
                c.setTime(parse);
                event.getHeaders().put("year", String.valueOf(c.get(Calendar.YEAR)));
                event.getHeaders().put("month", String.valueOf(c.get(Calendar.MONTH) + 1));
                event.getHeaders().put("day", String.valueOf(c.get(Calendar.DATE)));
                return event;
            } catch (ParseException var9) {
                throw new RuntimeException(var9);
            }
        }
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        public Builder() {
        }

        public void configure(Context context) {
        }

        public Interceptor build() {
            return new DateInterceptor();
        }
    }
}