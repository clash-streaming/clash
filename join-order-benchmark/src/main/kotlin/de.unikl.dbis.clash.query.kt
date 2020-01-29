import de.unikl.dbis.clash.query.Query
import de.unikl.dbis.clash.query.QueryBuilder

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
     * SELECT MIN(mc.note) AS production_note,
              MIN(t.title) AS movie_title,
              MIN(t.production_year) AS movie_year
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
        // queryBuilder.from(JOBConstants.COMPANY_TYPE, "ct")
        //     .from(JOBConstants.INFO_TYPE, "it")
        //     .from(JOBConstants.MOVIE_COMPANIES, "mc")
        //     .from(JOBConstants.MOVIE_INFO_IDX, "mi_idx")
        //     .from(JOBConstants.TITLE, "t")
        //     .where()
        TODO()
    }
}
