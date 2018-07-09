package ru.myx.xstore.s3.concept;

enum TreeSearchField {
	/**
	 *
	 */
	TYPE(true) {

		private final TreeSearchChecker EMPTY = new TreeSearchCheckerTypeEquals("");

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1) {
				return null;
			}
			switch (operation.charAt(0)) {
				case '=' :
					final int length = value.length();
					if (length == 0) {
						return this.EMPTY;
					}
					return new TreeSearchCheckerTypeEquals(value);
				case ':' :
					return new TreeSearchCheckerTypeLike(value);
				default :
					return null;
			}
		}
	},
	/**
	 *
	 */
	KEY(true) {

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1 || operation.charAt(0) != '=') {
				return null;
			}
			final int length = value.length();
			if (length == 0) {
				return null;
			}
			return new TreeSearchCheckerKey(value);
		}
	},
	/**
	 *
	 */
	TITLE(false) {

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			return null;
		}
	},
	/**
	 *
	 */
	OWNER(false) {

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			return null;
		}
	},
	/**
	 *
	 */
	FOLDER(true) {

		private final TreeSearchChecker FOLDER_YES = new TreeSearchCheckerFolder(true);

		private final TreeSearchChecker FOLDER_NO = new TreeSearchCheckerFolder(false);

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1 || operation.charAt(0) != '=') {
				return null;
			}
			final int length = value.length();
			if (length != 1) {
				return null;
			}
			final char c = value.charAt(0);
			return c == 'Y'
				? this.FOLDER_YES
				: c == 'N'
					? this.FOLDER_NO
					: null;
		}
	},
	/**
	 *
	 */
	LISTABLE(true) {

		private final TreeSearchChecker LIST_YES = new TreeSearchCheckerListable(true);

		private final TreeSearchChecker LIST_NO = new TreeSearchCheckerListable(false);

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1 || operation.charAt(0) != '=') {
				return null;
			}
			final int length = value.length();
			if (length != 1) {
				return null;
			}
			final char c = value.charAt(0);
			return c == 'Y'
				? this.LIST_YES
				: c == 'N'
					? this.LIST_NO
					: null;
		}
	},
	/**
	 *
	 */
	SEARCHABLE(true) {

		private final TreeSearchChecker SEARCH_YES = new TreeSearchCheckerSearchable(true);

		private final TreeSearchChecker SEARCH_NO = new TreeSearchCheckerSearchable(false);

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1 || operation.charAt(0) != '=') {
				return null;
			}
			final int length = value.length();
			if (length != 1) {
				return null;
			}
			final char c = value.charAt(0);
			return c == 'Y'
				? this.SEARCH_YES
				: c == 'N'
					? this.SEARCH_NO
					: null;
		}
	},
	/**
	 *
	 */
	STATE(true) {

		private final TreeSearchChecker[] CHECKERS = new TreeSearchChecker[]{
				new TreeSearchCheckerState(0), new TreeSearchCheckerState(1), new TreeSearchCheckerState(2), new TreeSearchCheckerState(3), new TreeSearchCheckerState(4),
				new TreeSearchCheckerState(5)
		};

		@Override
		TreeSearchChecker getChecker(final String operation, final String value) {

			if (value == null) {
				return null;
			}
			if (operation == null || operation.length() != 1 || operation.charAt(0) != '=') {
				return null;
			}
			final int length = value.length();
			if (length != 1) {
				return null;
			}
			final char c = value.charAt(0);
			if (!Character.isDigit(c)) {
				return null;
			}
			final int v = c - '0';
			if (v < 0 || v > 5) {
				return null;
			}
			return this.CHECKERS[v];
		}
	},

	;

	static final TreeSearchField getSearchField(final String field) {

		if (field == null) {
			return null;
		}
		final int length = field.length();
		if (length < 2 || field.charAt(0) != '$') {
			return null;
		}
		switch (field.charAt(1)) {
			case 't' :
				if (length == 5 && "$type".equals(field)) {
					return TYPE;
				}
				if (length == 6 && "$title".equals(field)) {
					return TITLE;
				}
				return null;
			case 'f' :
				if (length == 7 && "$folder".equals(field)) {
					return FOLDER;
				}
				return null;
			case 'o' :
				if (length == 6 && "$owner".equals(field)) {
					return OWNER;
				}
				return null;
			case 'k' :
				if (length == 4 && "$key".equals(field)) {
					return KEY;
				}
				return null;
			case 'l' :
				if (length == 9 && "$listable".equals(field)) {
					return LISTABLE;
				}
				return null;
			case 's' :
				if (length == 6 && "$state".equals(field)) {
					return STATE;
				}
				if (length == 11 && "$searchable".equals(field)) {
					return SEARCHABLE;
				}
				return null;
			default :
				return null;
		}
	}

	private final boolean locallySearchable;

	TreeSearchField(final boolean locallySearchable) {

		this.locallySearchable = locallySearchable;
	}

	abstract TreeSearchChecker getChecker(final String operation, final String value);

	final boolean isLocallySearchable() {

		return this.locallySearchable;
	}
}
