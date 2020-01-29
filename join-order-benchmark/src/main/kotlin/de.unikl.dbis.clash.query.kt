import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.QueryBuilder
import de.unikl.dbis.clash.query.parser.parsePredicate

// See https://github.com/gregrahn/join-order-benchmark

object JOBConstants {
    const val COMPANY_TYPE = "company_type"
    const val INFO_TYPE = "info_type"
    const val MOVIE_COMPANIES = "movie_companies"
    const val MOVIE_INFO_IDX = "movie_info_idx"
    const val TITLE = "title"
}

object JOBJoinsAndFilters {
    /**
     * SELECT *
       FROM company_type AS ct,
            info_type AS it,
            movie_companies AS mc,
            movie_info_idx AS mi_idx,
            title AS t
       WHERE ct.kind = 'production companies'
         AND it.info = 'top 250 rank'
         AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
         AND (mc.note LIKE '%(co-production)%'
             OR mc.note LIKE '%(presents)%')
         AND ct.id = mc.company_type_id
         AND t.id = mc.movie_id
         AND t.id = mi_idx.movie_id
         AND mc.movie_id = mi_idx.movie_id
         AND it.id = mi_idx.info_type_id;
     */
    fun q1a(): Query {
        val queryBuilder = QueryBuilder()
        queryBuilder.from(JOBConstants.COMPANY_TYPE, "ct")
            .from(JOBConstants.INFO_TYPE, "it")
            .from(JOBConstants.MOVIE_COMPANIES, "mc")
            .from(JOBConstants.MOVIE_INFO_IDX, "mi_idx")
            .from(JOBConstants.TITLE, "t")
            .where(parsePredicate("ct.kind = 'production companies'")!!)
            .where(parsePredicate("it.info = 'top 250 rank'")!!)
            .where(parsePredicate("mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'")!!)
            .where(parsePredicate("(mc.note LIKE '%(co-production)% OR mc.note LIKE '%(presents)%')")!!)
            .where(parsePredicate("ct.id = mc.company_type_id")!!)
            .where(parsePredicate("t.id = mc.movie_id")!!)
            .where(parsePredicate("t.id = mi_idx.movie_id")!!)
            .where(parsePredicate("mc.movie_id = mi_idx.movie_id")!!)
            .where(parsePredicate("it.id = mi_idx.info_type_id;")!!)
        return queryBuilder.build()
    }
}
