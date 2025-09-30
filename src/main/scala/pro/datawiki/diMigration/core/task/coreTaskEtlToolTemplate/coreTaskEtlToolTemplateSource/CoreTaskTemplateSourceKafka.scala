package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskEtlToolTemplateSourceKafka.{CoreTaskTemplateSourceKafkaListTopics, CoreTaskTemplateSourceKafkaTopic, CoreTaskTemplateSourceKafkaTopicsByRegexp}

case class CoreTaskTemplateSourceKafka(
                                        topics: CoreTaskTemplateSourceKafkaTopic,
                                        listTopics: CoreTaskTemplateSourceKafkaListTopics,
                                        topicsByRegexp: CoreTaskTemplateSourceKafkaTopicsByRegexp
                                      )