/**
 *
 */
package ru.myx.xstore.s3.concept;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae2.indexing.Condition;
import ru.myx.ae2.indexing.IndexingParser;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.query.OneCondition;
import ru.myx.query.SyntaxQuery;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.jdbc.RunnerTreeLoader;

/** @author myx */
public class TreeData extends RequestAttachment<TreeData, RunnerTreeLoader> {

	private static final Comparator<LinkData> COMPARATOR_ALPHABET;

	private static final Comparator<LinkData> COMPARATOR_TITLE_DESC;

	private static final Comparator<LinkData> COMPARATOR_HISTORY;

	private static final Comparator<LinkData> COMPARATOR_LISTING;

	private static final Comparator<LinkData> COMPARATOR_KEY_DESC;

	private static final Comparator<LinkData> COMPARATOR_CHANGED;

	private static final Comparator<LinkData> COMPARATOR_MODIFIED_ASC;

	private static final Comparator<LinkData> COMPARATOR_LOG;

	private static final Map<String, Number> EMPTY_MAP_ALPHABET;

	private static final Map<Integer, Map<Integer, Map<String, Number>>> EMPTY_MAP_CALENDAR;

	/**
	 */
	public static final LinkData[] ENTRY_ARRAY_EMPTY;

	private static final Map<String, String> EMPTY_NAMES;

	/**
	 *
	 */
	public static final TreeData EMPTY_TREE;

	private static final Map<String, String> REPLACEMENT_FIELDS;

	private static final Map<String, Function<OneCondition, OneCondition>> REPLACEMENT_FILTER_CONDITIONS;

	private static final int STATE_SYSTEM;

	private static final int STATE_ARCHIVE;

	private static final int STATE_PUBLISH;
	
	static {
		COMPARATOR_ALPHABET = new Comparator<LinkData>() {

			@Override
			public int compare(final LinkData o1, final LinkData o2) {

				return 0;
			}
		};

		COMPARATOR_TITLE_DESC = Collections.reverseOrder(TreeData.COMPARATOR_ALPHABET);

		COMPARATOR_HISTORY = new ComparatorHistory();

		COMPARATOR_LISTING = new ComparatorListing();

		COMPARATOR_KEY_DESC = Collections.reverseOrder(TreeData.COMPARATOR_LISTING);

		COMPARATOR_CHANGED = new ComparatorChanged();

		COMPARATOR_MODIFIED_ASC = Collections.reverseOrder(TreeData.COMPARATOR_CHANGED);

		COMPARATOR_LOG = new ComparatorLog();
		
		EMPTY_MAP_ALPHABET = Collections.emptyMap();

		EMPTY_MAP_CALENDAR = Collections.emptyMap();

		ENTRY_ARRAY_EMPTY = new LinkData[0];

		EMPTY_NAMES = Collections.emptyMap();

		EMPTY_TREE = new TreeData(null, TreeData.ENTRY_ARRAY_EMPTY, false, false);

		REPLACEMENT_FIELDS = TreeData.createReplacementFields();

		REPLACEMENT_FILTER_CONDITIONS = TreeData.createReplacementFilterConditions();

		STATE_SYSTEM = ModuleInterface.STATE_SYSTEM;

		STATE_ARCHIVE = ModuleInterface.STATE_ARCHIVE;

		STATE_PUBLISH = ModuleInterface.STATE_PUBLISH;
		
	}

	/** @return */
	public static final Map<String, String> createReplacementFields() {

		final Map<String, String> result = new HashMap<>();
		result.put("$guid", "t.lnkId");
		result.put("$state", "o.objState");
		result.put("$title", "o.objTitle");
		result.put("$key", "t.lnkName");
		result.put("$type", "o.objType");
		result.put("$created", "o.objCreated");
		result.put("$modified", "o.objDate");
		result.put("$folder", "t.lnkFolder");
		result.put("$listable", "o.objListable");
		result.put("$searchable", "o.objSearchable");
		return result;
	}

	/** @return */
	public static final Map<String, Function<OneCondition, OneCondition>> createReplacementFilterConditions() {

		final Map<String, Function<OneCondition, OneCondition>> result = new HashMap<>();
		final Function<OneCondition, OneCondition> toYesNo = new ReplacerConditionBooleanToChar();
		final Function<OneCondition, OneCondition> objState = new ReplacerConditionState();
		result.put("t.lnkFolder", toYesNo);
		result.put("$folder", toYesNo);
		result.put("$listable", toYesNo);
		result.put("$searchable", toYesNo);
		result.put("o.objState", objState);
		result.put("$state", objState);
		result.put("o.objListable", new ReplacerConditionStateList("(2,4)"));
		result.put("o.objSearchable", new ReplacerConditionStateList("(2,5)"));
		return result;
	}

	static void fillAlphabetStats(final Map<String, Number> map, final Map<String, String> alphabetConversion, final String defaultLetter, final String title) {

		final String letter = TreeData.getLetter(alphabetConversion, defaultLetter, title);
		final Number value = map.get(letter);
		if (value == null) {
			map.put(letter, new AtomicInteger(1));
		} else {
			((AtomicInteger) value).incrementAndGet();
		}
	}

	static void fillCalendarStats(final Map<Integer, Map<Integer, Map<String, Number>>> map, final Calendar calendar, final Date date, final String paramName) {

		calendar.setTime(date);
		final Integer yearDate = Reflect.getInteger(calendar.get(Calendar.YEAR));
		final Integer monthDate = Reflect.getInteger(calendar.get(Calendar.MONTH));
		final String dayDate = "created" + calendar.get(Calendar.DAY_OF_MONTH);
		Map<Integer, Map<String, Number>> yearMap = map.get(yearDate);
		if (yearMap == null) {
			map.put(yearDate, yearMap = Create.tempMap());
		}
		Map<String, Number> monthMap = yearMap.get(monthDate);
		if (monthMap == null) {
			yearMap.put(monthDate, monthMap = Create.tempMap());
		}
		{
			final Object value = monthMap.get(paramName);
			if (value == null) {
				monthMap.put(paramName, new AtomicInteger(1));
			} else {
				((AtomicInteger) value).incrementAndGet();
			}
		}
		{
			final Number value = monthMap.get(dayDate);
			if (value == null) {
				monthMap.put(dayDate, new AtomicInteger(1));
			} else {
				((AtomicInteger) value).incrementAndGet();
			}
		}
	}

	/** @param alphabetConversion
	 * @param defaultLetter
	 * @param title
	 * @return letter */
	public static final String getLetter(final Map<String, String> alphabetConversion, final String defaultLetter, final String title) {

		if (title == null) {
			return defaultLetter;
		}
		final int length = title.length();
		for (int i = 0; i < length; ++i) {
			final char c = title.charAt(i);
			if (Character.isLetterOrDigit(c)) {
				return Convert.MapEntry.toString(alphabetConversion, String.valueOf(c), defaultLetter);
			}
		}
		return defaultLetter;
	}

	/** @param sort
	 * @return sort */
	public static final Comparator<LinkData> getListingSort(final String sort) {

		if (sort == null) {
			return TreeData.COMPARATOR_HISTORY;
		}
		final int length = sort.length();
		if (length == 0) {
			return TreeData.COMPARATOR_HISTORY;
		}
		switch (sort.charAt(0)) {
			case 'a' :
				if (length == 8 && "alphabet".equals(sort)) {
					return TreeData.COMPARATOR_ALPHABET;
				}
				break;
			case 'c' :
				if (length == 7 && "changed".equals(sort)) {
					return TreeData.COMPARATOR_CHANGED;
				}
				break;
			case 'h' :
				if (length == 7 && "history".equals(sort)) {
					return TreeData.COMPARATOR_HISTORY;
				}
				break;
			case 'l' :
				if (length == 3 && "log".equals(sort)) {
					return TreeData.COMPARATOR_LOG;
				}
				if (length == 7 && "listing".equals(sort)) {
					return TreeData.COMPARATOR_LISTING;
				}
				break;
			case '$' :
				if (length > 1) {
					switch (sort.charAt(1)) {
						case 'c' :
							if (length == 8 && "$created".equals(sort)) {
								return TreeData.COMPARATOR_LOG;
							}
							if (length == 9 && "$created-".equals(sort)) {
								return TreeData.COMPARATOR_HISTORY;
							}
							if (length == 9 && "$created+".equals(sort)) {
								return TreeData.COMPARATOR_LOG;
							}
							break;
						case 'k' :
							if (length == 4 && "$key".equals(sort)) {
								return TreeData.COMPARATOR_LISTING;
							}
							if (length == 5 && "$key+".equals(sort)) {
								return TreeData.COMPARATOR_LISTING;
							}
							if (length == 5 && "$key-".equals(sort)) {
								return TreeData.COMPARATOR_KEY_DESC;
							}
							break;
						case 'm' :
							if (length == 9 && "$modified".equals(sort)) {
								return TreeData.COMPARATOR_MODIFIED_ASC;
							}
							if (length == 10 && "$modified+".equals(sort)) {
								return TreeData.COMPARATOR_MODIFIED_ASC;
							}
							if (length == 10 && "$modified-".equals(sort)) {
								return TreeData.COMPARATOR_CHANGED;
							}
							break;
						case 't' :
							if (length == 6 && "$title".equals(sort)) {
								return TreeData.COMPARATOR_ALPHABET;
							}
							if (length == 7 && "$title-".equals(sort)) {
								return TreeData.COMPARATOR_TITLE_DESC;
							}
							if (length == 7 && "$title+".equals(sort)) {
								return TreeData.COMPARATOR_ALPHABET;
							}
							break;
						default :
					}
				}
				break;
			default :
		}

		final String trimmed = sort.trim();
		if (trimmed.length() != length) {
			return TreeData.getListingSort(trimmed);
		}
		Report.warning("TREE-DATA", "Unkonwn sort order (" + Format.Describe.toEcmaSource(sort, "") + ") requested, using \"history\"");
		return TreeData.COMPARATOR_HISTORY;
	}

	private static final LinkData[] searchLocalAlphabetJava(final int limit,
			final boolean all,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final LinkData[] entries,
			final boolean asc) {

		if (entries == null) {
			return null;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return null;
		}
		final List<LinkData> result = new ArrayList<>();
		int left = limit <= 0
			? Integer.MAX_VALUE
			: limit;
		if (asc) {
			for (int i = 0; i < entryCount; ++i) {
				final LinkData current = entries[i];
				if ((all || current.listable) && TreeData.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
					result.add(current);
					if (--left == 0) {
						break;
					}
				}
			}
		} else {
			for (int i = entryCount - 1; i >= 0; --i) {
				final LinkData current = entries[i];
				if ((all || current.listable) && TreeData.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
					result.add(current);
					if (--left == 0) {
						break;
					}
				}
			}
		}
		return result.isEmpty()
			? null
			: result.toArray(new LinkData[result.size()]);
	}

	private static final Map<String, Number>
			searchLocalFillAlphabetStatsJava(final LinkData[] entries, final boolean all, final Map<String, String> alphabetConversion, final String defaultLetter) {

		final Map<String, Number> result = Create.tempMap();
		if (entries == null) {
			return result;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return result;
		}
		for (int i = entries.length - 1; i >= 0; --i) {
			final LinkData current = entries[i];
			if (all || current.listable) {
				TreeData.fillAlphabetStats(result, alphabetConversion, defaultLetter, current.letter);
			}
		}
		return result;
	}

	private static final Map<Integer, Map<Integer, Map<String, Number>>>
			searchLocalFillCalendarStatsJava(final LinkData[] entries, final boolean all, final long startDate, final long endDate) {

		final Map<Integer, Map<Integer, Map<String, Number>>> result = Create.tempMap();
		if (entries == null) {
			return result;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return result;
		}
		final Calendar calendar = Calendar.getInstance();
		final Date date = new Date();
		final long compareStartDate = startDate == -1L
			? Long.MIN_VALUE
			: startDate;
		final long compareEndDate = endDate == -1L
			? Long.MAX_VALUE
			: endDate;
		for (int i = entryCount - 1; i >= 0; --i) {
			final LinkData current = entries[i];
			if (all || current.searchable) {
				final long created = current.objCreated;
				if (created >= compareStartDate && created < compareEndDate) {
					date.setTime(created);
					TreeData.fillCalendarStats(result, calendar, date, "created");
				}
			}
		}
		return result;
	}

	/** @return false when search should be made in database */
	private static final boolean searchLocalIterateJava(final LinkData[] entries,
			final long startDate,
			final long endDate,
			final boolean filterListable,
			final boolean filterSearchable,
			final String query,
			final boolean asc,
			final TreeSearchCallback callback) {

		if (entries == null) {
			return true;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return true;
		}
		final long compareStartDate = startDate == -1L
			? Long.MIN_VALUE
			: startDate;
		final long compareEndDate = endDate == -1L
			? Long.MAX_VALUE
			: endDate;
		final List<Object> parsed = SyntaxQuery.parseFilter(null, null, null, TreeData.REPLACEMENT_FILTER_CONDITIONS, false, query);
		final Condition[][] normalized = IndexingParser.normalize(parsed, 0);
		final OneCondition[][] optimized1 = IndexingParser.optimize(normalized[0]);
		final OneCondition[][] optimized2 = IndexingParser.optimize(normalized[1]);
		final TreeSearchChecker[][] check1;
		final TreeSearchChecker[][] check2;
		if (optimized1 == null) {
			check1 = null;
		} else {
			check1 = new TreeSearchChecker[optimized1.length][];
			for (int i = optimized1.length - 1; i >= 0; --i) {
				final OneCondition[] set = optimized1[i];
				check1[i] = new TreeSearchChecker[set.length];
				for (int j = set.length - 1; j >= 0; j--) {
					final OneCondition condition = set[j];
					final TreeSearchField field = TreeSearchField.getSearchField(condition.getField());
					if (field == null) {
						throw new IllegalArgumentException("Invalid search field name (" + condition.getField() + ")!");
					}
					if (!field.isLocallySearchable()) {
						return false;
					}
					final TreeSearchChecker checker = field.getChecker(condition.getOperator(), condition.getValue());
					if (checker == null) {
						return false;
					}
					check1[i][j] = checker;
				}
			}
		}
		if (optimized2 == null) {
			check2 = null;
		} else {
			check2 = new TreeSearchChecker[optimized2.length][];
			for (int i = optimized2.length - 1; i >= 0; --i) {
				final OneCondition[] set = optimized2[i];
				check2[i] = new TreeSearchChecker[set.length];
				for (int j = set.length - 1; j >= 0; j--) {
					final OneCondition condition = set[j];
					final TreeSearchField field = TreeSearchField.getSearchField(condition.getField());
					if (field == null) {
						throw new IllegalArgumentException("Invalid search field name (" + condition.getField() + ")!");
					}
					if (!field.isLocallySearchable()) {
						return false;
					}
					final TreeSearchChecker checker = field.getChecker(condition.getOperator(), condition.getValue());
					if (checker == null) {
						return false;
					}
					check2[i][j] = checker;
				}
			}
		}
		// filter
		final int start = asc
			? 0
			: entryCount - 1;
		final int step = asc
			? 1
			: -1;
		for (int index = start, left = entryCount; left > 0; left--, index += step) {
			final LinkData current = entries[index];
			final long created = current.objCreated;
			if (!((filterListable || current.listable) && (filterSearchable || current.searchable) && created >= compareStartDate && created < compareEndDate)) {
				continue;
			}
			if (check2 != null) {
				boolean matchOr = false;
				for (int i = check2.length - 1; i >= 0; --i) {
					boolean matchAnd = true;
					final TreeSearchChecker[] line = check2[i];
					for (int j = line.length - 1; j >= 0; j--) {
						if (!line[j].check(current)) {
							matchAnd = false;
							break;
						}
					}
					if (matchAnd) {
						matchOr = true;
						break;
					}
				}
				if (matchOr) {
					continue;
				}
			}
			if (check1 != null) {
				boolean matchOr = false;
				for (int i = check1.length - 1; i >= 0; --i) {
					boolean matchAnd = true;
					final TreeSearchChecker[] line = check1[i];
					for (int j = line.length - 1; j >= 0; j--) {
						if (!line[j].check(current)) {
							matchAnd = false;
							break;
						}
					}
					if (matchAnd) {
						matchOr = true;
						break;
					}
				}
				if (!matchOr) {
					continue;
				}
			}
			if (callback.execute(current)) {
				return true;
			}
		}
		return true;
	}

	private static final LinkData[] searchLocalJava(final int limit,
			final long startDate,
			final long endDate,
			final boolean filterListable,
			final boolean filterSearchable,
			final LinkData[] entries,
			final boolean asc) {

		if (entries == null) {
			return TreeData.ENTRY_ARRAY_EMPTY;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return TreeData.ENTRY_ARRAY_EMPTY;
		}
		if (startDate == -1L && endDate == -1L && filterListable && filterSearchable) {
			if (limit <= 0 || limit >= entryCount) {
				if (asc) {
					return entries;
				}
				final LinkData[] result = new LinkData[entryCount];
				for (int i = entryCount - 1, j = 0; i >= 0; i--, j++) {
					result[j] = entries[i];
				}
				return result;
			}
			final LinkData[] result = new LinkData[limit];
			if (asc) {
				System.arraycopy(entries, 0, result, 0, limit);
				return result;
			}
			for (int i = 0, j = entryCount - 1; i < limit; i++, j--) {
				result[i] = entries[j];
			}
			return result;
		}
		final long compareStartDate = startDate == -1L
			? Long.MIN_VALUE
			: startDate;
		final long compareEndDate = endDate == -1L
			? Long.MAX_VALUE
			: endDate;
		final List<LinkData> result = new ArrayList<>();
		int left = limit <= 0
			? Integer.MAX_VALUE
			: limit;
		if (asc) {
			for (int i = 0; i < entryCount; ++i) {
				final LinkData current = entries[i];
				final long created = current.objCreated;
				if ((filterListable || current.listable) && (filterSearchable || current.searchable) && created >= compareStartDate && created < compareEndDate) {
					result.add(current);
					if (--left == 0) {
						break;
					}
				}
			}
		} else {
			for (int i = entryCount - 1; i >= 0; --i) {
				final LinkData current = entries[i];
				final long created = current.objCreated;
				if ((filterListable || current.listable) && (filterSearchable || current.searchable) && created >= compareStartDate && created < compareEndDate) {
					result.add(current);
					if (--left == 0) {
						break;
					}
				}
			}
		}
		return result.isEmpty()
			? TreeData.ENTRY_ARRAY_EMPTY
			: (LinkData[]) result.toArray(new LinkData[result.size()]);
	}

	private Map<String, Number> alphabetListable = null;

	private Map<Integer, Map<Integer, Map<String, Number>>> calendarSearchable = null;

	/**
	 *
	 */
	public boolean invalid = false;

	/** guid key of this link */
	public final String lnkId;

	/** loaded/cached tree elements */
	public LinkData[] tree;

	/** cached tree files */
	private LinkData[] treeFiles;

	/** cached tree files, sorted */
	private LinkData[] treeFilesHistory;

	/** cached tree folders */
	private LinkData[] treeFolders;

	/** cached tree sorted */
	private LinkData[] treeHistory;

	/**
	 *
	 */
	private Map<String, String> treeNames;

	/** @param lnkId
	 */
	public TreeData(final String lnkId) {

		this.lnkId = lnkId;
	}

	/** @param lnkId
	 * @param treeEntries
	 * @param allFiles
	 * @param allFolders
	 */
	public TreeData(final String lnkId, final LinkData[] treeEntries, final boolean allFiles, final boolean allFolders) {

		this.lnkId = lnkId;
		this.tree = treeEntries;
		if (treeEntries == null || treeEntries.length == 0) {
			this.tree = TreeData.ENTRY_ARRAY_EMPTY;
			this.treeNames = TreeData.EMPTY_NAMES;
			this.treeHistory = TreeData.ENTRY_ARRAY_EMPTY;
			this.treeFiles = TreeData.ENTRY_ARRAY_EMPTY;
			this.treeFilesHistory = TreeData.ENTRY_ARRAY_EMPTY;
			this.treeFolders = TreeData.ENTRY_ARRAY_EMPTY;
			this.calendarSearchable = TreeData.EMPTY_MAP_CALENDAR;
			this.alphabetListable = TreeData.EMPTY_MAP_ALPHABET;
		} else {
			if (allFiles) {
				this.treeFiles = treeEntries;
			}
			if (allFolders) {
				this.treeFolders = treeEntries;
			}
		}
	}

	@Override
	public final TreeData apply(final RunnerTreeLoader ctx) {

		try {
			final PreparedStatement ps = ctx.getStatement();
			ps.clearParameters();
			ps.setString(1, this.lnkId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<LinkData> entries = new ArrayList<>();
					boolean allFiles = true;
					boolean allFolders = true;
					do {
						final String lnkId = rs.getString(1);
						final String lnkName = rs.getString(2);
						final String dbFolder = rs.getString(3);
						final boolean lnkFolder = dbFolder != null && dbFolder.length() > 0 && 'Y' == dbFolder.charAt(0);
						final String objTitle = rs.getString(4);
						final long objCreated = rs.getTimestamp(5).getTime();
						final long objModified = rs.getTimestamp(6).getTime();
						final String objType = rs.getString(7);
						final int objState = rs.getInt(8);
						final boolean listable = objState == TreeData.STATE_SYSTEM || objState == TreeData.STATE_PUBLISH;
						final boolean searchable = objState == TreeData.STATE_ARCHIVE || objState == TreeData.STATE_PUBLISH;
						allFiles &= !lnkFolder;
						allFolders &= lnkFolder;
						final LinkData linkData = new LinkData(
								lnkId,
								-1,
								null,
								lnkName,
								lnkFolder,
								null,
								null,
								objTitle,
								objCreated,
								objModified,
								null,
								objType,
								objState,
								listable,
								searchable,
								null);
						entries.add(linkData);
					} while (rs.next());
					this.tree = entries.toArray(new LinkData[entries.size()]);
					if (allFiles) {
						this.treeFiles = this.tree;
					}
					if (allFolders) {
						this.treeFolders = this.tree;
					}
					this.setResult(this);
					return this;
				}
				this.tree = TreeData.ENTRY_ARRAY_EMPTY;
				this.treeHistory = TreeData.ENTRY_ARRAY_EMPTY;
				this.treeFiles = TreeData.ENTRY_ARRAY_EMPTY;
				this.treeFilesHistory = TreeData.ENTRY_ARRAY_EMPTY;
				this.treeFolders = TreeData.ENTRY_ARRAY_EMPTY;
				this.calendarSearchable = TreeData.EMPTY_MAP_CALENDAR;
				this.alphabetListable = TreeData.EMPTY_MAP_ALPHABET;
				this.setResult(TreeData.EMPTY_TREE);
				return TreeData.EMPTY_TREE;
			}
		} catch (final SQLException e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	/** @param name
	 * @return guid */
	public final String getChildGuid(final String name) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		{
			final Map<String, String> treeNames = this.treeNames;
			if (treeNames != null) {
				return this.treeNames.get(name);
			}
		}
		{
			final Map<String, String> treeNames;
			locked : synchronized (this) {
				if (this.treeNames != null) {
					treeNames = this.treeNames;
					break locked;
				}
				final int treeLength = this.tree.length;
				if (treeLength == 0) {
					this.treeNames = TreeData.EMPTY_NAMES;
					return null;
				}
				if (treeLength == 1) {
					final LinkData data = this.tree[0];
					treeNames = this.treeNames = Collections.singletonMap(data.lnkName, data.lnkId);
				} else {
					final Map<String, String> temp = treeLength < 256
						? new TreeMap<>()
						: new HashMap<>(treeLength);
					for (int i = treeLength - 1; i >= 0; --i) {
						final LinkData data = this.tree[i];
						temp.put(data.lnkName, data.lnkId);
					}
					treeNames = this.treeNames = temp;
				}
			}
			return treeNames.get(name);
		}
	}

	/** @param storage
	 * @param link
	 * @param limit
	 * @param sort
	 * @return children */
	public final LinkData[] getChildren(final StorageLevel3 storage, final LinkData link, final int limit, final String sort) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (sort == null || sort.trim().length() == 0) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesHistory(), true);
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.tree, true);
		}
		if (order == TreeData.COMPARATOR_LOG) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesHistory(), false);
		}
		if (order == TreeData.COMPARATOR_HISTORY) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesHistory(), true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.tree, false);
		}
		if (order == TreeData.COMPARATOR_CHANGED) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesChanged(), true);
		}
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesChanged(), false);
		}
		if (order == TreeData.COMPARATOR_LISTING) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesDirectory(), true);
		}
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesDirectory(), false);
		}
		Report.warning("TREE-DATA", "Unkonwn sort order requested: " + sort);
		return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetEntriesHistory(), true);
	}

	/** @param storage
	 * @param link
	 * @param limit
	 * @param sort
	 * @return children */
	public final LinkData[] getChildrenListable(final StorageLevel3 storage, final LinkData link, final int limit, final String sort) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (sort == null || sort.trim().length() == 0) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesHistory(), true);
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.tree, true);
		}
		if (order == TreeData.COMPARATOR_LOG) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesHistory(), false);
		}
		if (order == TreeData.COMPARATOR_HISTORY) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesHistory(), true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.tree, false);
		}
		if (order == TreeData.COMPARATOR_CHANGED) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesChanged(), true);
		}
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesChanged(), false);
		}
		if (order == TreeData.COMPARATOR_LISTING) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesDirectory(), true);
		}
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesDirectory(), false);
		}
		Report.warning("TREE-DATA", "Unkonwn sort order (" + Format.Describe.toEcmaSource(sort, "") + ") requested, using \"history\"");
		return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetEntriesHistory(), true);
	}

	/** @param storage
	 * @param link
	 * @param limit
	 * @param sort
	 * @return files */
	public final LinkData[] getFiles(final StorageLevel3 storage, final LinkData link, final int limit, final String sort) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (sort == null || sort.trim().length() == 0) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesHistory(), true);
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFiles(), true);
		}
		if (order == TreeData.COMPARATOR_LOG) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesHistory(), false);
		}
		if (order == TreeData.COMPARATOR_HISTORY) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesHistory(), true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFiles(), false);
		}
		if (order == TreeData.COMPARATOR_CHANGED) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesChanged(), true);
		}
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesChanged(), false);
		}
		if (order == TreeData.COMPARATOR_LISTING) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesDirectory(), true);
		}
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesDirectory(), false);
		}
		Report.warning("TREE-DATA", "Unkonwn sort order (" + Format.Describe.toEcmaSource(sort, "") + ") requested, using \"history\"");
		return TreeData.searchLocalJava(limit, -1L, -1L, true, true, this.internGetFilesHistory(), true);
	}

	/** @param storage
	 * @param link
	 * @param limit
	 * @param sort
	 * @return files */
	public final LinkData[] getFilesListable(final StorageLevel3 storage, final LinkData link, final int limit, final String sort) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (sort == null || sort.trim().length() == 0) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesHistory(), true);
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFiles(), true);
		}
		if (order == TreeData.COMPARATOR_LOG) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesHistory(), false);
		}
		if (order == TreeData.COMPARATOR_HISTORY) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesHistory(), true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFiles(), false);
		}
		if (order == TreeData.COMPARATOR_CHANGED) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesChanged(), true);
		}
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesChanged(), false);
		}
		if (order == TreeData.COMPARATOR_LISTING) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesDirectory(), true);
		}
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesDirectory(), false);
		}
		Report.warning("TREE-DATA", "Unkonwn sort order (" + Format.Describe.toEcmaSource(sort, "") + ") requested, using \"history\"");
		return TreeData.searchLocalJava(limit, -1L, -1L, false, true, this.internGetFilesHistory(), true);
	}

	/** @return folders */
	public final LinkData[] getFolders() {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		return this.internGetFolders();
	}

	/** @return folders */
	public final LinkData[] getFoldersListable() {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		return TreeData.searchLocalJava(0, -1L, -1L, false, true, this.internGetFolders(), true);
	}

	@Override
	public String getKey() {

		return this.lnkId;
	}

	/** @return linkData */
	private final LinkData[] internGetEntriesChanged() {

		final int length = this.tree.length;
		if (length <= 1) {
			return this.tree;
		}
		final LinkData[] entriesChanged = new LinkData[length];
		System.arraycopy(this.tree, 0, entriesChanged, 0, length);
		Arrays.sort(entriesChanged, TreeData.COMPARATOR_CHANGED);
		return entriesChanged;
	}

	/** @return linkData */
	private final LinkData[] internGetEntriesDirectory() {

		final int length = this.tree.length;
		if (length <= 1) {
			return this.tree;
		}
		final LinkData[] entriesChanged = new LinkData[length];
		System.arraycopy(this.tree, 0, entriesChanged, 0, length);
		Arrays.sort(entriesChanged, TreeData.COMPARATOR_LISTING);
		return entriesChanged;
	}

	/** @return linkData */
	private final LinkData[] internGetEntriesHistory() {

		if (this.treeHistory == null) {
			synchronized (this.tree) {
				if (this.treeHistory == null) {
					final int length = this.tree.length;
					if (length <= 1) {
						return this.treeHistory = this.tree;
					}
					final LinkData[] entriesHistory = new LinkData[length];
					System.arraycopy(this.tree, 0, entriesHistory, 0, length);
					Arrays.sort(entriesHistory, TreeData.COMPARATOR_HISTORY);
					return this.treeHistory = entriesHistory;
				}
			}
		}
		return this.treeHistory;
	}

	private final LinkData[] internGetFiles() {

		if (this.treeFiles == null) {
			synchronized (this.tree) {
				if (this.treeFiles == null) {
					final List<LinkData> result = new ArrayList<>();
					final int entryCount = this.tree.length;
					for (int i = 0; i < entryCount; ++i) {
						final LinkData current = this.tree[i];
						if (!current.lnkFolder) {
							result.add(current);
						}
					}
					this.treeFiles = result.isEmpty()
						? TreeData.ENTRY_ARRAY_EMPTY
						: (LinkData[]) result.toArray(new LinkData[result.size()]);
				}
			}
		}
		return this.treeFiles;
	}

	private final LinkData[] internGetFilesChanged() {

		final LinkData[] files = this.internGetFiles();
		final int length = files.length;
		if (length <= 1) {
			return files;
		}
		final LinkData[] filesChanged = new LinkData[length];
		System.arraycopy(files, 0, filesChanged, 0, length);
		Arrays.sort(filesChanged, TreeData.COMPARATOR_CHANGED);
		return filesChanged;
	}

	private final LinkData[] internGetFilesDirectory() {

		final LinkData[] files = this.internGetFiles();
		final int length = files.length;
		if (length <= 1) {
			return files;
		}
		final LinkData[] filesChanged = new LinkData[length];
		System.arraycopy(files, 0, filesChanged, 0, length);
		Arrays.sort(filesChanged, TreeData.COMPARATOR_LISTING);
		return filesChanged;
	}

	private final LinkData[] internGetFilesHistory() {

		if (this.treeFilesHistory == null) {
			final LinkData[] files = this.internGetFiles();
			synchronized (files) {
				if (this.treeFilesHistory == null) {
					final int length = files.length;
					if (length <= 1) {
						return this.treeFilesHistory = files;
					}
					final LinkData[] filesHistory = new LinkData[length];
					System.arraycopy(this.treeFiles, 0, filesHistory, 0, length);
					Arrays.sort(filesHistory, TreeData.COMPARATOR_HISTORY);
					return this.treeFilesHistory = filesHistory;
				}
			}
		}
		return this.treeFilesHistory;
	}

	private final LinkData[] internGetFolders() {

		if (this.treeFolders == null) {
			synchronized (this.tree) {
				if (this.treeFolders == null) {
					final List<LinkData> result = new ArrayList<>();
					final int entryCount = this.tree.length;
					for (int i = 0; i < entryCount; ++i) {
						final LinkData current = this.tree[i];
						if (current.lnkFolder) {
							result.add(current);
						}
					}
					this.treeFolders = result.isEmpty()
						? TreeData.ENTRY_ARRAY_EMPTY
						: (LinkData[]) result.toArray(new LinkData[result.size()]);
				}
			}
		}
		return this.treeFolders;
	}

	/** @param storage
	 * @param limit
	 * @param all
	 * @param sort
	 * @param startDate
	 * @param endDate
	 * @param query
	 * @return identifiers */
	public final LinkData[]
			searchLocal(final StorageLevel3 storage, final int limit, final boolean all, final String sort, final long startDate, final long endDate, final String query) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		final LinkData[] filterSource;
		final boolean filterAscending;
		if (order == TreeData.COMPARATOR_ALPHABET) {
			filterSource = this.tree;
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			filterSource = this.tree;
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_LOG) {
			filterSource = this.internGetEntriesHistory();
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_HISTORY) {
			filterSource = this.internGetEntriesHistory();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_CHANGED) {
			filterSource = this.internGetEntriesChanged();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			filterSource = this.internGetEntriesChanged();
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_LISTING) {
			filterSource = this.internGetEntriesDirectory();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			filterSource = this.internGetEntriesDirectory();
			filterAscending = false;
		} else {
			throw new IllegalStateException("Shouldn't be here!");
		}
		if (query == null || query.trim().length() == 0) {
			return TreeData.searchLocalJava(limit, startDate, endDate, true, all, filterSource, filterAscending);
		}
		final List<LinkData> local = new ArrayList<>(filterSource.length);
		if (TreeData.searchLocalIterateJava(filterSource, startDate, endDate, true, all, query, filterAscending, new TreeSearchCallback() {

			private int left = limit <= 0
				? Integer.MAX_VALUE
				: limit;

			@Override
			final boolean execute(final LinkData link) {

				local.add(link);
				return --this.left <= 0;
			}
		})) {
			return local.isEmpty()
				? null
				: local.toArray(new LinkData[local.size()]);
		}
		final String condition = SyntaxQuery.filterToWhere(null, null, TreeData.REPLACEMENT_FIELDS, TreeData.REPLACEMENT_FILTER_CONDITIONS, false, query);
		final LinkData[] entries = storage.getServerInterface().searchLocal(this.lnkId, condition);
		if (entries == null) {
			return null;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return null;
		}
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalJava(limit, startDate, endDate, true, all, entries, true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalJava(limit, startDate, endDate, true, all, entries, false);
		}
		if (limit <= 0 || limit >= entryCount) {
			final LinkData[] filtered = TreeData.searchLocalJava(limit, startDate, endDate, true, all, entries, true);
			if (order == null) {
				throw new IllegalStateException("Shouldn't be here!");
			}
			if (filtered != entries) {
				Arrays.sort(filtered, order);
				return filtered;
			}
			final LinkData[] entriesSorted = new LinkData[filtered.length];
			System.arraycopy(filtered, 0, entriesSorted, 0, filtered.length);
			Arrays.sort(entriesSorted, order);
			return entriesSorted;
		}
		final LinkData[] entriesSorted = new LinkData[entryCount];
		System.arraycopy(entries, 0, entriesSorted, 0, entryCount);
		if (order == null) {
			throw new IllegalStateException("Shouldn't be here!");
		}
		Arrays.sort(entriesSorted, order);
		return TreeData.searchLocalJava(limit, startDate, endDate, true, all, entriesSorted, true);
	}

	/** @param storage
	 * @param limit
	 * @param all
	 * @param sort
	 * @param alphabetConversion
	 * @param defaultLetter
	 * @param filterLetter
	 * @param query
	 * @return identifiers */
	public final LinkData[] searchLocalAlphabet(final StorageLevel3 storage,
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		final Comparator<LinkData> order = TreeData.getListingSort(sort);
		final LinkData[] filterSource;
		final boolean filterAscending;
		if (order == TreeData.COMPARATOR_ALPHABET) {
			filterSource = this.tree;
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			filterSource = this.tree;
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_LOG) {
			filterSource = this.internGetEntriesHistory();
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_HISTORY) {
			filterSource = this.internGetEntriesHistory();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_CHANGED) {
			filterSource = this.internGetEntriesChanged();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_MODIFIED_ASC) {
			filterSource = this.internGetEntriesChanged();
			filterAscending = false;
		} else //
		if (order == TreeData.COMPARATOR_LISTING) {
			filterSource = this.internGetEntriesDirectory();
			filterAscending = true;
		} else //
		if (order == TreeData.COMPARATOR_KEY_DESC) {
			filterSource = this.internGetEntriesDirectory();
			filterAscending = false;
		} else {
			throw new IllegalStateException("Shouldn't be here!");
		}
		if (query == null || query.trim().length() == 0) {
			return TreeData.searchLocalAlphabetJava(limit, all, alphabetConversion, defaultLetter, filterLetter, filterSource, filterAscending);
		}
		final List<LinkData> local = new ArrayList<>(filterSource.length);
		if (TreeData.searchLocalIterateJava(filterSource, -1L, -1L, all, true, query, filterAscending, new TreeSearchCallback() {

			private int left = limit <= 0
				? Integer.MAX_VALUE
				: limit;

			@Override
			final boolean execute(final LinkData link) {

				if (TreeData.getLetter(alphabetConversion, defaultLetter, link.letter).equals(filterLetter)) {
					local.add(link);
					return --this.left <= 0;
				}
				return false;
			}
		})) {
			return local.isEmpty()
				? null
				: local.toArray(new LinkData[local.size()]);
		}
		final String condition = SyntaxQuery.filterToWhere(null, null, TreeData.REPLACEMENT_FIELDS, TreeData.REPLACEMENT_FILTER_CONDITIONS, false, query);
		final LinkData[] entries = storage.getServerInterface().searchLocal(this.lnkId, condition);
		if (entries == null) {
			return null;
		}
		final int entryCount = entries.length;
		if (entryCount == 0) {
			return null;
		}
		if (order == TreeData.COMPARATOR_ALPHABET) {
			return TreeData.searchLocalAlphabetJava(limit, all, alphabetConversion, defaultLetter, filterLetter, entries, true);
		}
		if (order == TreeData.COMPARATOR_TITLE_DESC) {
			return TreeData.searchLocalAlphabetJava(limit, all, alphabetConversion, defaultLetter, filterLetter, entries, false);
		}
		if (limit <= 0 || limit >= entryCount) {
			final LinkData[] filtered = TreeData.searchLocalAlphabetJava(limit, all, alphabetConversion, defaultLetter, filterLetter, entries, true);
			if (order == null) {
				throw new IllegalStateException("Shouldn't be here!");
			}
			if (filtered != entries) {
				Arrays.sort(filtered, order);
				return filtered;
			}
			final LinkData[] entriesSorted = new LinkData[filtered.length];
			System.arraycopy(filtered, 0, entriesSorted, 0, filtered.length);
			Arrays.sort(entriesSorted, order);
			return entriesSorted;
		}
		final LinkData[] entriesSorted = new LinkData[entryCount];
		System.arraycopy(entries, 0, entriesSorted, 0, entryCount);
		if (order == null) {
			throw new IllegalStateException("Shouldn't be here!");
		}
		Arrays.sort(entriesSorted, order);
		return TreeData.searchLocalAlphabetJava(limit, all, alphabetConversion, defaultLetter, filterLetter, entriesSorted, true);
	}

	/** @param storage
	 * @param all
	 * @param alphabetConversion
	 * @param defaultLetter
	 * @param query
	 * @return alphabet */
	public final Map<String, Number>
			searchLocalAlphabetStats(final StorageLevel3 storage, final boolean all, final Map<String, String> alphabetConversion, final String defaultLetter, final String query) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (query == null || query.trim().length() == 0) {
			if (all) {
				return TreeData.searchLocalFillAlphabetStatsJava(this.tree, true, alphabetConversion, defaultLetter);
			}
			if (this.alphabetListable == null) {
				synchronized (this.tree) {
					if (this.alphabetListable == null) {
						return this.alphabetListable = TreeData.searchLocalFillAlphabetStatsJava(this.tree, false, alphabetConversion, defaultLetter);
					}
				}
			}
			return this.alphabetListable;
		}
		final Map<String, Number> result = Create.tempMap();
		if (TreeData.searchLocalIterateJava(this.tree, -1L, -1L, all, true, query, true, new TreeSearchCallback() {

			@Override
			final boolean execute(final LinkData link) {

				TreeData.fillAlphabetStats(result, alphabetConversion, defaultLetter, link.letter);
				return false;
			}
		})) {
			return result;
		}
		final String condition = SyntaxQuery.filterToWhere(null, null, TreeData.REPLACEMENT_FIELDS, TreeData.REPLACEMENT_FILTER_CONDITIONS, false, query);
		final LinkData[] entries = storage.getServerInterface().searchLocal(this.lnkId, condition);
		return TreeData.searchLocalFillAlphabetStatsJava(entries, all, alphabetConversion, defaultLetter);
	}

	/** @param storage
	 * @param all
	 * @param startDate
	 * @param endDate
	 * @param query
	 * @return calendar */
	public final Map<Integer, Map<Integer, Map<String, Number>>>
			searchLocalCalendarStats(final StorageLevel3 storage, final boolean all, final long startDate, final long endDate, final String query) {

		if (this.tree == TreeData.ENTRY_ARRAY_EMPTY) {
			return null;
		}
		if (query == null || query.trim().length() == 0) {
			if (!all && startDate == -1L && endDate == -1L) {
				if (this.calendarSearchable == null) {
					synchronized (this.tree) {
						if (this.calendarSearchable == null) {
							return this.calendarSearchable = TreeData.searchLocalFillCalendarStatsJava(this.tree, false, -1L, -1L);
						}
					}
				}
				return this.calendarSearchable;
			}
			return TreeData.searchLocalFillCalendarStatsJava(this.tree, all, startDate, endDate);
		}
		final Map<Integer, Map<Integer, Map<String, Number>>> result = Create.tempMap();
		final Calendar calendar = Calendar.getInstance();
		final Date date = new Date();
		if (TreeData.searchLocalIterateJava(this.tree, startDate, endDate, true, all, query, true, new TreeSearchCallback() {

			@Override
			final boolean execute(final LinkData link) {

				date.setTime(link.objCreated);
				TreeData.fillCalendarStats(result, calendar, date, "created");
				return false;
			}
		})) {
			return result;
		}
		final String condition = SyntaxQuery.filterToWhere(null, null, TreeData.REPLACEMENT_FIELDS, TreeData.REPLACEMENT_FILTER_CONDITIONS, false, query);
		final LinkData[] entries = storage.getServerInterface().searchLocal(this.lnkId, condition);
		return TreeData.searchLocalFillCalendarStatsJava(entries, all, startDate, endDate);
	}
}
