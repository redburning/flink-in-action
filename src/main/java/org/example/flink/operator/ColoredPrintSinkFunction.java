package org.example.flink.operator;

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.example.flink.data.UserAction;

public class ColoredPrintSinkFunction extends PrintSinkFunction<UserAction> {

	private static final long serialVersionUID = 1L;
	
	public static final String ANSI_GREEN = "\u001B[32m";
	public static final String ANSI_RESET = "\u001B[0m";
	
	@Override
    public void invoke(UserAction userAction, Context context) {
		String coloredValue = "{\n  user: " + ANSI_GREEN + "{" + "\n    " + "id: " + userAction.getUser().getId() + ",\n    "
				+ "name: " + userAction.getUser().getName() + ",\n    " + "age: " + userAction.getUser().getAge() + ",\n    "
				+ "email: " + userAction.getUser().getEmail() + "\n  }" + ANSI_RESET + ",\n  productId: "
				+ userAction.getProductId() + ",\n  eventType: " + userAction.getEventType() + ",\n  timestamp: "
				+ userAction.getTimestamp() + "\n}";
		System.out.println(coloredValue);
    }

}
