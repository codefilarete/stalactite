TODO: 
- Mutualize ContainerDescription Iterator between MetaModelPathComponentBuilder#DescriptionIterator and MetaModelPathIterator#buildMetaModelIterator
Can't be mutualized because we loose MetaModel information

- Change signature of IMetaModelTransformer.tranform to use more-targeted generic: use Generics on <? extends ContainerDescription>
Too complex, not sure it bring a lot

- Find a way to check if wicket path is valid: wicket ids present in hierarchy: must instantiate component, maybe risky, even if Session is needed. See WiketTester for Help ?
- 