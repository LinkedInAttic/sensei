package com.senseidb.indexing;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;

public class NegativeAwareDecimalFormat extends DecimalFormat {
	private final MetaType metaType;

	public NegativeAwareDecimalFormat(MetaType metaType, String pattern,
			DecimalFormatSymbols symbols) {
		super(pattern, symbols);
		this.metaType = metaType;
		// TODO Auto-generated constructor stub
	}

	@Override
	public StringBuffer format(double number, StringBuffer result,
			FieldPosition fieldPosition) {
		if (number < 0) {
			number = getMinDoubleForType(metaType) - number;
		}
		return super.format(number, result, fieldPosition);
	}

	private double getMinDoubleForType(MetaType metaType2) {
		if (metaType == MetaType.Double)
			return Short.MIN_VALUE;
		if (metaType == MetaType.Float)
			return Integer.MIN_VALUE;

		throw new UnsupportedOperationException(
				"Only Double and Float are supported");
	}

	@Override
	public StringBuffer format(long number, StringBuffer result,
			FieldPosition fieldPosition) {
		if (number < 0) {
			number = getMinLongForType(metaType) - number;
		}
		return super.format(number, result, fieldPosition);
	}

	private long getMinLongForType(MetaType metaType) {
		if (metaType == MetaType.Short)
			return Short.MIN_VALUE;
		if (metaType == MetaType.Integer)
			return Integer.MIN_VALUE;
		if (metaType == MetaType.Long)
			return Long.MIN_VALUE;
		throw new UnsupportedOperationException(
				"Only Long, Integer and Short are supported");
	}
}
